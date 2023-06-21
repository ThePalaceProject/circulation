from __future__ import annotations

import sys
from typing import Any, List

from pydantic import Field, NonNegativeInt

from core.util.flask_util import CustomBaseModel

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class StatisticsBaseModel(CustomBaseModel):
    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)

    def __add__(self, other: Self) -> Self:
        """Sum each property and return new instance."""
        return self.__class__(
            **{field: self[field] + other[field] for field in self.__fields__.keys()}
        )

    @classmethod
    def zeroed(cls) -> Self:
        """An instance of this class with all values set to zero."""
        return cls(**{field: 0 for field in cls.__fields__.keys()})


class PatronStatistics(StatisticsBaseModel):
    """Patron statistics."""

    total: NonNegativeInt = Field(description="Number of patrons.")
    with_active_loan: NonNegativeInt = Field(
        description="Number of patrons with an active loan."
    )
    with_active_loan_or_hold: NonNegativeInt = Field(
        description="Number of patrons with one or more loans or holds (or both)."
    )
    loans: NonNegativeInt = Field(
        description="Number of loans for all associated patrons."
    )
    holds: NonNegativeInt = Field(
        description="Number of holds for all associated patrons."
    )


class InventoryStatistics(StatisticsBaseModel):
    """Inventory statistics."""

    titles: NonNegativeInt = Field(description="Number of books.")
    available_titles: NonNegativeInt = Field(
        description="Number of books available to lend."
    )
    self_hosted_titles: NonNegativeInt = Field(
        description="Number of books that are self-hosted."
    )
    open_access_titles: NonNegativeInt = Field(
        description="Number of books with an Open Access license."
    )
    licensed_titles: NonNegativeInt = Field(
        description="Number of licensed books (either metered or unlimited)."
    )
    unlimited_license_titles: NonNegativeInt = Field(
        description="Number of books with an unlimited license."
    )
    metered_license_titles: NonNegativeInt = Field(
        description="Number of books with a metered (counted) license."
    )
    metered_licenses_owned: NonNegativeInt = Field(
        description="Metered licenses owned."
    )
    metered_licenses_available: NonNegativeInt = Field(
        description="Metered licenses currently available."
    )


class LibraryStatistics(CustomBaseModel):
    """Statistics for a library."""

    key: str = Field(
        description="Short name for library, which can be used as a key.",
    )
    name: str = Field(description="Library name.")
    patron_statistics: PatronStatistics = Field(
        description="Patron statistics for this library."
    )
    inventory_summary: InventoryStatistics = Field(
        description="Summary of inventory statistics for this library."
    )
    collection_ids: List[int] = Field(
        description="List of associated collection identifiers."
    )


class CollectionInventory(CustomBaseModel):
    """Collection inventory."""

    id: NonNegativeInt = Field(description="Collection identifier.")
    name: str = Field(description="Collection name.")
    inventory: InventoryStatistics = Field(
        description="Inventory statistics for this collection."
    )


class StatisticsResponse(CustomBaseModel):
    """Statistics response for authorized libraries and collections."""

    collections: List[CollectionInventory] = Field(
        description="List of collection-level statistics (includes collections not associated with a library."
    )
    libraries: List[LibraryStatistics] = Field(
        description="List of library-level statistics."
    )
    inventory_summary: InventoryStatistics = Field(
        description="Summary inventory across all included collections."
    )
    patron_summary: PatronStatistics = Field(
        description="Summary patron statistics across all libraries."
    )

    @property
    def libraries_by_key(self) -> dict[str, LibraryStatistics]:
        """Dictionary of library statistics keyed by their `key` value.2"""
        return {lib.key: lib for lib in self.libraries}
