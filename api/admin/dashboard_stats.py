from __future__ import annotations

import typing
from datetime import datetime
from functools import partial
from typing import Callable, Iterable

from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import and_, or_

from api.admin.model.dashboard_statistics import (
    CollectionInventory,
    InventoryStatistics,
    LibraryStatistics,
    PatronStatistics,
    StatisticsResponse,
)
from core.model import Admin, Collection, Hold, Library, LicensePool, Loan, Patron


def generate_statistics(admin: Admin, db: Session) -> StatisticsResponse:
    return Statistics(db).stats(admin)


class Statistics:
    METERED_LICENSE_FILTER = and_(  # type: ignore[type-var]
        LicensePool.licenses_owned > 0,
        LicensePool.unlimited_access == False,
        LicensePool.open_access == False,
    )
    UNLIMITED_LICENSE_FILTER = and_(  # type: ignore[type-var]
        LicensePool.unlimited_access == True,
        LicensePool.open_access == False,
    )
    OPEN_ACCESS_FILTER = LicensePool.open_access == True
    SELF_HOSTED_FILTER = LicensePool.self_hosted == True
    AT_LEAST_ONE_LENDABLE_FILTER = or_(
        UNLIMITED_LICENSE_FILTER,
        OPEN_ACCESS_FILTER,
        and_(METERED_LICENSE_FILTER, LicensePool.licenses_available > 0),
    )

    def __init__(self, session: Session):
        self._db = session

    def _libraries_for_admin(self, admin: Admin) -> list[Library]:
        """Return a list of libraries to which this user has access."""
        return [
            library
            for library in self._db.query(Library)
            if admin.is_librarian(library)
        ]

    def _collection_count(self, collection_filter, query_filter) -> int:
        return (
            self._db.query(LicensePool)
            .filter(collection_filter)
            .filter(query_filter)
            .count()
        )

    def _gather_collection_stats(self, collection: Collection) -> CollectionInventory:
        collection_filter = LicensePool.collection_id == collection.id
        _count: Callable = partial(self._collection_count, collection_filter)

        metered_license_title_count = _count(self.METERED_LICENSE_FILTER)
        unlimited_license_title_count = _count(self.UNLIMITED_LICENSE_FILTER)
        open_access_title_count = _count(self.OPEN_ACCESS_FILTER)
        self_hosted_title_count = _count(self.SELF_HOSTED_FILTER)
        at_least_one_loanable_count = _count(self.AT_LEAST_ONE_LENDABLE_FILTER)

        licenses_owned_count, licenses_available_count = map(
            lambda x: x if x is not None else 0,
            self._db.query(
                func.sum(LicensePool.licenses_owned),
                func.sum(LicensePool.licenses_available),
            )
            .filter(collection_filter)
            .filter(self.METERED_LICENSE_FILTER)
            .all()[0],
        )

        return CollectionInventory(
            id=collection.id,  # type: ignore[arg-type]
            name=collection.name,  # type: ignore[arg-type]
            inventory=InventoryStatistics(
                titles=metered_license_title_count
                + unlimited_license_title_count
                + open_access_title_count,
                available_titles=at_least_one_loanable_count,
                self_hosted_titles=self_hosted_title_count,
                open_access_titles=open_access_title_count,
                licensed_titles=metered_license_title_count
                + unlimited_license_title_count,
                unlimited_license_titles=unlimited_license_title_count,
                metered_license_titles=metered_license_title_count,
                metered_licenses_owned=licenses_owned_count,
                metered_licenses_available=licenses_available_count,
            ),
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

    def _collection_level_statistics(
        self,
        collections: typing.Collection[Collection],
    ) -> tuple[list[CollectionInventory], InventoryStatistics]:
        """Return individual and summary statistics for the given collections.

        The list of per-collection statistics is sorted by the collection `id`.
        """
        collection_stats = [self._gather_collection_stats(c) for c in collections]
        summary_stats = sum(
            (c.inventory for c in collection_stats), InventoryStatistics.zeroed()
        )
        return sorted(collection_stats, key=lambda c: c.id), summary_stats

    @staticmethod
    def lookup_stats(
        collection_inventories: Iterable[CollectionInventory],
        collections: Iterable[Collection],
        defaults: Iterable[InventoryStatistics] | None = None,
    ) -> Iterable[InventoryStatistics]:
        """Return the inventory dictionaries for the specified collections."""
        defaults = defaults if defaults is not None else [InventoryStatistics.zeroed()]
        collection_ids = {c.id for c in collections}
        return (
            (
                stats.inventory
                for stats in collection_inventories
                if stats.id in collection_ids
            )
            if collection_ids
            else defaults
        )

    def stats(self, admin: Admin) -> StatisticsResponse:
        """Build and return a statistics response for user's authorized libraries."""

        # Determine which libraries and collections are authorized for this user.
        authorized_libraries = self._libraries_for_admin(admin)
        authorized_collections_by_library = {
            lib.short_name: set(lib.all_collections) for lib in authorized_libraries
        }
        all_authorized_collections: list[Collection] = [
            c for c in self._db.query(Collection) if admin.can_see_collection(c)
        ]

        # Gather collection-level statistics for authorized collections.
        (
            collection_inventories,
            collection_inventory_summary,
        ) = self._collection_level_statistics(all_authorized_collections)

        # Gather library-level statistics for the authorized libraries by
        # summing up the values of each of libraries associated collections.
        inventory_by_library = {
            library_key: sum(
                self.lookup_stats(collection_inventories, collections),
                InventoryStatistics.zeroed(),
            )
            for library_key, collections in authorized_collections_by_library.items()
        }
        patron_stats_by_library = {
            lib.short_name: self._gather_patron_stats(lib)
            for lib in authorized_libraries
        }
        library_statistics = [
            LibraryStatistics(
                key=lib.short_name,  # type: ignore[arg-type]
                name=lib.name or "(missing library name)",
                patron_statistics=patron_stats_by_library[lib.short_name],
                inventory_summary=inventory_by_library[lib.short_name],
                collection_ids=sorted(
                    [c.id for c in authorized_collections_by_library[lib.short_name]]
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
            patron_summary=patron_summary,
        )
