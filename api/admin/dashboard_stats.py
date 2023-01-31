from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import and_, distinct, join, select

from core.model import Admin, Collection, Hold, Library, LicensePool, Loan, Patron


def generate_statistics(admin: Admin, _db: Session):
    library_stats = {}

    total_title_count = 0
    total_license_count = 0
    total_available_license_count = 0

    collection_counts = dict()
    for collection in _db.query(Collection):
        if not admin or not admin.can_see_collection(collection):
            continue

        licensed_title_count = (
            _db.query(LicensePool)
            .filter(LicensePool.collection_id == collection.id)
            .filter(
                and_(
                    LicensePool.licenses_owned > 0,
                    LicensePool.open_access == False,
                )
            )
            .count()
        )

        open_title_count = (
            _db.query(LicensePool)
            .filter(LicensePool.collection_id == collection.id)
            .filter(LicensePool.open_access == True)
            .count()
        )

        # The sum queries return None instead of 0 if there are
        # no license pools in the db.

        license_count = (
            _db.query(func.sum(LicensePool.licenses_owned))
            .filter(LicensePool.collection_id == collection.id)
            .filter(
                LicensePool.open_access == False,
            )
            .all()[0][0]
            or 0
        )

        available_license_count = (
            _db.query(func.sum(LicensePool.licenses_available))
            .filter(LicensePool.collection_id == collection.id)
            .filter(
                LicensePool.open_access == False,
            )
            .all()[0][0]
            or 0
        )

        total_title_count += licensed_title_count + open_title_count
        total_license_count += license_count
        total_available_license_count += available_license_count

        collection_counts[collection.name] = dict(
            licensed_titles=licensed_title_count,
            open_access_titles=open_title_count,
            licenses=license_count,
            available_licenses=available_license_count,
        )

    for library in _db.query(Library):
        # Only include libraries this admin has librarian access to.
        if not admin or not admin.is_librarian(library):
            continue

        patron_count = _db.query(Patron).filter(Patron.library_id == library.id).count()

        active_loans_patron_count = (
            _db.query(distinct(Patron.id))
            .join(Patron.loans)
            .filter(
                Loan.end >= datetime.now(),
            )
            .filter(Patron.library_id == library.id)
            .count()
        )

        active_patrons = (
            select([Patron.id])
            .select_from(
                join(
                    Loan,
                    Patron,
                    and_(
                        Patron.id == Loan.patron_id,
                        Patron.library_id == library.id,
                        Loan.id != None,
                        Loan.end >= datetime.now(),
                    ),
                )
            )
            .union(
                select([Patron.id]).select_from(
                    join(
                        Hold,
                        Patron,
                        and_(
                            Patron.id == Hold.patron_id,
                            Patron.library_id == library.id,
                            Hold.id != None,
                        ),
                    )
                )
            )
            .alias()
        )

        active_loans_or_holds_patron_count_query = select(
            [func.count(distinct(active_patrons.c.id))]
        ).select_from(active_patrons)

        result = _db.execute(active_loans_or_holds_patron_count_query)
        active_loans_or_holds_patron_count = [r[0] for r in result][0]

        loan_count = (
            _db.query(Loan)
            .join(Loan.patron)
            .filter(Patron.library_id == library.id)
            .filter(Loan.end >= datetime.now())
            .count()
        )

        hold_count = (
            _db.query(Hold)
            .join(Hold.patron)
            .filter(Patron.library_id == library.id)
            .count()
        )

        title_count = 0
        license_count = 0
        available_license_count = 0

        library_collection_counts = dict()
        for collection in library.all_collections:
            # sometimes a parent collection may be dissociated from a library
            # in this case we may not have access to the collection as a library staff member
            if collection.name not in collection_counts:
                continue

            counts = collection_counts[collection.name]
            library_collection_counts[collection.name] = counts
            title_count += counts.get("licensed_titles", 0) + counts.get(
                "open_access_titles", 0
            )
            license_count += counts.get("licenses", 0)
            available_license_count += counts.get("available_licenses", 0)

        library_stats[library.short_name] = dict(
            patrons=dict(
                total=patron_count,
                with_active_loans=active_loans_patron_count,
                with_active_loans_or_holds=active_loans_or_holds_patron_count,
                loans=loan_count,
                holds=hold_count,
            ),
            inventory=dict(
                titles=title_count,
                licenses=license_count,
                available_licenses=available_license_count,
            ),
            collections=library_collection_counts,
        )

    total_patrons = sum(
        stats.get("patrons", {}).get("total", 0)
        for stats in list(library_stats.values())
    )
    total_with_active_loans = sum(
        stats.get("patrons", {}).get("with_active_loans", 0)
        for stats in list(library_stats.values())
    )
    total_with_active_loans_or_holds = sum(
        stats.get("patrons", {}).get("with_active_loans_or_holds", 0)
        for stats in list(library_stats.values())
    )

    total_loans = sum(
        stats.get("patrons", {}).get("loans", 0)
        for stats in list(library_stats.values())
    )
    total_holds = sum(
        stats.get("patrons", {}).get("holds", 0)
        for stats in list(library_stats.values())
    )

    library_stats["total"] = dict(
        patrons=dict(
            total=total_patrons,
            with_active_loans=total_with_active_loans,
            with_active_loans_or_holds=total_with_active_loans_or_holds,
            loans=total_loans,
            holds=total_holds,
        ),
        inventory=dict(
            titles=total_title_count,
            licenses=total_license_count,
            available_licenses=total_available_license_count,
        ),
        collections=collection_counts,
    )

    return library_stats
