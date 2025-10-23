from __future__ import annotations

import dataclasses
import datetime
from typing import Self

from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.format import FormatData
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.licensing import License, LicensePool
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron


class LoanAndHoldInfoMixin:
    collection_id: int
    identifier_type: str
    identifier: str

    def collection(self, _db: Session) -> Collection:
        """Find the Collection to which this object belongs."""
        collection = Collection.by_id(_db, self.collection_id)
        if collection is None:
            raise PalaceValueError(
                f"collection_id {self.collection_id} could not be found."
            )
        return collection

    def license_pool(self, _db: Session) -> LicensePool:
        """Find the LicensePool model object corresponding to this object."""
        collection = self.collection(_db)
        pool, is_new = LicensePool.for_foreign_id(
            _db,
            collection.data_source,
            self.identifier_type,
            self.identifier,
            collection=collection,
        )
        return pool


@dataclasses.dataclass(kw_only=True)
class LoanInfo(LoanAndHoldInfoMixin):
    """A record of a loan."""

    collection_id: int
    identifier_type: str
    identifier: str
    start_date: datetime.datetime | None = None
    end_date: datetime.datetime | None
    external_identifier: str | None = None
    locked_to: FormatData | None = None
    available_formats: set[FormatData] | None = None
    license_identifier: str | None = None

    @classmethod
    def from_license_pool(
        cls,
        license_pool: LicensePool,
        *,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None,
        external_identifier: str | None = None,
        locked_to: FormatData | None = None,
        available_formats: set[FormatData] | None = None,
        license_identifier: str | None = None,
    ) -> Self:
        collection_id = license_pool.collection_id
        assert collection_id is not None
        identifier_type = license_pool.identifier.type
        assert identifier_type is not None
        identifier = license_pool.identifier.identifier
        assert identifier is not None
        return cls(
            collection_id=collection_id,
            identifier_type=identifier_type,
            identifier=identifier,
            start_date=start_date,
            end_date=end_date,
            external_identifier=external_identifier,
            locked_to=locked_to,
            available_formats=available_formats,
            license_identifier=license_identifier,
        )

    def __repr__(self) -> str:
        return "<LoanInfo for {}/{}, start={} end={}>".format(
            self.identifier_type,
            self.identifier,
            self.start_date.isoformat() if self.start_date else self.start_date,
            self.end_date.isoformat() if self.end_date else self.end_date,
        )

    def create_or_update(
        self, patron: Patron, license_pool: LicensePool | None = None
    ) -> tuple[Loan, bool]:
        session = Session.object_session(patron)
        license_pool = license_pool or self.license_pool(session)

        loanable: LicensePool | License
        if self.license_identifier is not None:
            loanable = session.execute(
                select(License).where(
                    License.identifier == self.license_identifier,
                    License.license_pool == license_pool,
                )
            ).scalar_one()
        else:
            loanable = license_pool

        loan, is_new = loanable.loan_to(
            patron,
            start=self.start_date,
            end=self.end_date,
            external_identifier=self.external_identifier,
        )
        db = Session.object_session(patron)

        if self.available_formats:
            # We have extra information about the formats that are available
            # for this licensepool. Sometimes we only get this information
            # when looking up a loan (e.g. Overdrive) so we capture this
            # information here.
            for format in self.available_formats:
                format.apply(db, license_pool.data_source, license_pool.identifier)

        if self.locked_to is not None:
            # The loan source is letting us know that the loan is
            # locked to a specific delivery mechanism. Even if
            # this is the first we've heard of this loan,
            # it may have been created in another app or through
            # a library-website integration.
            self.locked_to.apply_to_loan(db, loan)
        return loan, is_new


@dataclasses.dataclass(kw_only=True)
class HoldInfo(LoanAndHoldInfoMixin):
    """A record of a hold.

    :param identifier_type: Ex. Identifier.BIBLIOTHECA_ID.
    :param identifier: Expected to be the unicode string of the isbn, etc.
    :param start_date: When the patron made the reservation.
    :param end_date: When reserved book is expected to become available.
        Expected to be passed in date, not unicode format.
    :param hold_position:  Patron's place in the hold line. When not available,
        default to be passed is None, which is equivalent to "first in line".
    """

    collection_id: int
    identifier_type: str
    identifier: str
    start_date: datetime.datetime | None = None
    end_date: datetime.datetime | None = None
    hold_position: int | None

    @classmethod
    def from_license_pool(
        cls,
        license_pool: LicensePool,
        *,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        hold_position: int | None,
    ) -> Self:
        collection_id = license_pool.collection_id
        assert collection_id is not None
        identifier_type = license_pool.identifier.type
        assert identifier_type is not None
        identifier = license_pool.identifier.identifier
        assert identifier is not None
        return cls(
            collection_id=collection_id,
            identifier_type=identifier_type,
            identifier=identifier,
            start_date=start_date,
            end_date=end_date,
            hold_position=hold_position,
        )

    def __repr__(self) -> str:
        return "<HoldInfo for {}/{}, start={} end={}, position={}>".format(
            self.identifier_type,
            self.identifier,
            self.start_date.isoformat() if self.start_date else self.start_date,
            self.end_date.isoformat() if self.end_date else self.end_date,
            self.hold_position,
        )

    def create_or_update(
        self, patron: Patron, license_pool: LicensePool | None = None
    ) -> tuple[Hold, bool]:
        session = Session.object_session(patron)
        license_pool = license_pool or self.license_pool(session)
        return license_pool.on_hold_to(
            patron,
            start=self.start_date,
            end=self.end_date,
            position=self.hold_position,
        )
