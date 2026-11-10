from __future__ import annotations

import dataclasses
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime

from sqlalchemy import not_, union
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select, func, select
from sqlalchemy.sql.expression import and_, distinct, or_

from palace.manager.api.admin.model.dashboard_statistics import (
    CollectionInventory,
    InventoryStatistics,
    LibraryStatistics,
    PatronStatistics,
    StatisticsResponse,
)
from palace.manager.integration.goals import Goals
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool, LicensePoolType
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron


def generate_statistics(admin: Admin, db: Session) -> StatisticsResponse:
    return Statistics(db).stats(admin)


class Statistics:
    METERED_LICENSE_FILTER = and_(  # type: ignore[type-var]
        LicensePool.metered_or_equivalent_type, LicensePool.active_status
    )
    UNLIMITED_LICENSE_FILTER = and_(  # type: ignore[type-var]
        LicensePool.unlimited_non_open_access_type, LicensePool.active_status
    )
    OPEN_ACCESS_FILTER = and_(  # type: ignore[type-var]
        LicensePool.unlimited_open_access_type, LicensePool.active_status
    )
    AT_LEAST_ONE_LOANABLE_FILTER = or_(
        UNLIMITED_LICENSE_FILTER,
        OPEN_ACCESS_FILTER,
        and_(METERED_LICENSE_FILTER, LicensePool.licenses_available > 0),
    )
    # The union of every filter above. Rows excluded by this contribute nothing to any
    # of the statistics, so the single inventory query can be narrowed to it up front.
    COUNTED_FILTER = and_(  # type: ignore[type-var]
        LicensePool.active_status,
        or_(
            LicensePool.metered_or_equivalent_type,
            LicensePool.type == LicensePoolType.UNLIMITED,
        ),
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
        ) = self._authorized_collections(admin, authorized_libraries)

        collection_inventories = self._create_collection_inventories(
            all_authorized_collections
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

    def _collections_by_library(
        self, authorized_libraries: list[Library]
    ) -> dict[int, set[int]]:
        """Map each library's id to the ids of the collections associated with it.

        This answers for every library at once what ``Library.associated_collections``
        answers for one, so the caller does not emit a query per library.

        :param authorized_libraries: Libraries to look up associated collections for.
        :return: Mapping of library id to the set of associated collection ids.
        """
        library_ids = [lib.id for lib in authorized_libraries if lib.id is not None]
        if not library_ids:
            return {}

        rows = self._db.execute(
            select(IntegrationLibraryConfiguration.library_id, Collection.id)
            .select_from(Collection)
            .join(IntegrationConfiguration)
            .join(IntegrationLibraryConfiguration)
            .where(
                IntegrationConfiguration.goal == Goals.LICENSE_GOAL,
                IntegrationLibraryConfiguration.library_id.in_(library_ids),
            )
        ).all()

        collections_by_library: dict[int, set[int]] = defaultdict(set)
        for library_id, collection_id in rows:
            collections_by_library[library_id].add(collection_id)
        return collections_by_library

    def _authorized_collections(
        self,
        admin: Admin,
        authorized_libraries: list[Library],
    ) -> tuple[list[_Collection], dict[str, list[_Collection]]]:
        authorized_collections: set[_Collection] = set()
        authorized_collections_by_library = {}

        all_collections = self._all_collections()
        collection_ids_by_library = self._collections_by_library(authorized_libraries)

        for library in authorized_libraries:
            library_collections = {
                all_collections[collection_id]
                for collection_id in collection_ids_by_library.get(library.id, ())
                if collection_id in all_collections
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

        return (
            sorted(authorized_collections, key=lambda c: c.id),
            authorized_collections_by_library,
        )

    def _collections_statistics_query(
        self, collections: list[_Collection]
    ) -> dict[int, dict[str, dict[str, int]]]:
        """Gather every inventory statistic for the given collections in one pass.

        Each statistic is a conditional aggregate over the same scan, so the
        ``licensepools``/``editions`` join is performed once rather than once per
        statistic.

        :param collections: Collections to restrict the query to.
        :return: Mapping of collection id to medium to statistic name to value.
        """
        query = (
            select(
                LicensePool.collection_id,
                Edition.medium,
                func.count()
                .filter(self.METERED_LICENSE_FILTER)
                .label("metered_titles"),
                func.count()
                .filter(self.UNLIMITED_LICENSE_FILTER)
                .label("unlimited_titles"),
                func.count()
                .filter(self.OPEN_ACCESS_FILTER)
                .label("open_access_titles"),
                func.count()
                .filter(self.AT_LEAST_ONE_LOANABLE_FILTER)
                .label("loanable_titles"),
                func.coalesce(
                    func.sum(LicensePool.licenses_owned).filter(
                        self.METERED_LICENSE_FILTER
                    ),
                    0,
                ).label("metered_licenses_owned"),
                func.coalesce(
                    func.sum(LicensePool.licenses_available).filter(
                        self.METERED_LICENSE_FILTER
                    ),
                    0,
                ).label("metered_licenses_available"),
            )
            .select_from(LicensePool)
            .join(Edition, Edition.id == LicensePool.presentation_edition_id)
            .where(
                self.COUNTED_FILTER,
                LicensePool.collection_id.in_({c.id for c in collections}),
            )
            .group_by(Edition.medium, LicensePool.collection_id)
        )

        statistics: dict[int, dict[str, dict[str, int]]] = defaultdict(dict)
        for row in self._db.execute(query).mappings().all():
            statistics[row["collection_id"]][row["medium"]] = {
                k: v for k, v in row.items() if k not in ("medium", "collection_id")
            }
        return statistics

    def _run_collections_stats_queries(
        self, collections: list[_Collection]
    ) -> dict[int | None, _CollectionStatisticsQueryResults]:
        statistics = self._collections_statistics_query(collections)
        return {
            c.id: _CollectionStatisticsQueryResults(
                statistics_by_medium=statistics.get(c.id) or {}
            )
            for c in collections
        }

    def _create_collection_inventories(
        self, collections: list[_Collection]
    ) -> list[CollectionInventory]:
        statistics = self._run_collections_stats_queries(collections)
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
    """The inventory statistics for a single collection, keyed by medium.

    The inner mapping holds one entry per statistic, using the column labels
    produced by ``Statistics._collections_statistics_query``.
    """

    statistics_by_medium: dict[str, dict[str, int]]

    def inventories_by_medium(self) -> dict[str, InventoryStatistics]:
        """Return a mapping of all mediums present to their associated inventories."""
        return {
            medium: self.inventory_for_medium(medium)
            for medium in self.mediums_present()
        }

    def mediums_present(self) -> set[str]:
        """Returns the set of the mediums present in these collection statistics."""
        return set(self.statistics_by_medium)

    def inventory_for_medium(self, medium: str) -> InventoryStatistics:
        """Return statistics for the specified medium."""
        statistics = self.statistics_by_medium.get(medium, {})
        unlimited_titles = statistics.get("unlimited_titles", 0)
        open_access_titles = statistics.get("open_access_titles", 0)
        loanable_titles = statistics.get("loanable_titles", 0)
        metered_titles = statistics.get("metered_titles", 0)

        return InventoryStatistics(
            titles=metered_titles + unlimited_titles + open_access_titles,
            available_titles=loanable_titles,
            open_access_titles=open_access_titles,
            licensed_titles=metered_titles + unlimited_titles,
            unlimited_license_titles=unlimited_titles,
            metered_license_titles=metered_titles,
            metered_licenses_owned=statistics.get("metered_licenses_owned", 0),
            metered_licenses_available=statistics.get("metered_licenses_available", 0),
        )


@dataclasses.dataclass(frozen=True)
class _Collection:
    id: int
    name: str
