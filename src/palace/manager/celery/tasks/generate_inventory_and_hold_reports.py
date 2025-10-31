from __future__ import annotations

import csv
import logging
import tempfile
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import IO, Any

from celery import shared_task
from sqlalchemy import bindparam, case, func, lateral, not_, select, true
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import Select, Subquery
from sqlalchemy.sql.selectable import Lateral

from palace.manager.celery.task import Task
from palace.manager.integration.goals import Goals
from palace.manager.integration.license.opds.opds1.settings import OPDSImporterSettings
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.email.email import SendEmailCallable
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.service.storage.s3 import S3Service
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Equivalency, Identifier
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import License, LicensePool
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.sqlalchemy.model.work import Work, WorkGenre
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.log import elapsed_time_logging
from palace.manager.util.uuid import uuid_encode


def eligible_integrations(
    integrations: list[IntegrationConfiguration], registry: LicenseProvidersRegistry
) -> list[IntegrationConfiguration]:
    """Subset a list of integrations to only those that are eligible for the inventory report."""

    def is_eligible(integration: IntegrationConfiguration) -> bool:
        if not integration.collection.is_active:
            return False
        settings_cls = registry[integration.protocol].settings_class()
        return issubclass(settings_cls, OPDSImporterSettings)

    return [integration for integration in integrations if is_eligible(integration)]


def library_report_integrations(
    library: Library, session: Session, registry: LicenseProvidersRegistry
) -> list[IntegrationConfiguration]:
    """Return a list of collection integrations to report for the given library."""

    integrations = session.scalars(
        select(IntegrationConfiguration)
        .join(IntegrationLibraryConfiguration)
        .where(
            IntegrationLibraryConfiguration.library_id == library.id,
            IntegrationConfiguration.goal == Goals.LICENSE_GOAL,
            not_(
                IntegrationConfiguration.settings_dict.contains(
                    {"include_in_inventory_report": False}
                )
            ),
        )
    ).all()
    return sorted(
        eligible_integrations(integrations, registry), key=lambda i: i.name or ""
    )


log = logging.getLogger(__name__)


def generate_report(
    session: Session,
    library_id: int,
    email_address: str,
    send_email: SendEmailCallable,
    registry: LicenseProvidersRegistry,
    s3_service: S3Service,
) -> None:
    library = get_one(session, Library, id=library_id)

    if not library:
        log.error(
            f"Cannot generate inventory and holds report for library (id={library_id}): "
            f"library not found."
        )
        return

    log.info(
        f"Starting inventory and holds report job for {library.name}({library.short_name})."
    )

    current_time = datetime.now()
    date_str = current_time.strftime("%Y-%m-%d_%H:%M:%s")

    file_name_modifier = f"{library.short_name}-{date_str}"

    integration_ids = [
        integration.id
        for integration in library_report_integrations(
            library=library, session=session, registry=registry
        )
    ]

    # generate inventory report csv file
    sql_params: dict[str, Any] = {
        "library_id": library.id,
        "integration_ids": tuple(integration_ids),
    }

    with tempfile.NamedTemporaryFile() as report_zip:
        zip_path = Path(report_zip.name)

        with (
            create_temp_file() as inventory_report_file,
            create_temp_file() as inventory_activity_report_file,
            create_temp_file() as holds_with_no_licenses_report_file,
        ):
            generate_csv_report(
                session,
                csv_file=inventory_report_file,
                sql_params=sql_params,
                query=inventory_report_query(),
            )

            generate_csv_report(
                session,
                csv_file=inventory_activity_report_file,
                sql_params=sql_params,
                query=palace_inventory_activity_report_query(),
            )

            generate_csv_report(
                session,
                csv_file=holds_with_no_licenses_report_file,
                sql_params=sql_params,
                query=holds_with_no_licenses_report_query(),
            )

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.write(
                    filename=inventory_activity_report_file.name,
                    arcname=f"palace-inventory-activity-report-for-library-{file_name_modifier}.csv",
                )
                archive.write(
                    filename=inventory_report_file.name,
                    arcname=f"palace-inventory-report-for-library-{file_name_modifier}.csv",
                )
                archive.write(
                    filename=holds_with_no_licenses_report_file.name,
                    arcname=f"palace-holds-with-no-licenses-report-for-library-{file_name_modifier}.csv",
                )

            uid = uuid_encode(uuid.uuid4())
            key = (
                f"{S3Service.DOWNLOADS_PREFIX}/inventory_and_holds/{library.short_name}/"
                f"inventory-and-holds-for-library-{file_name_modifier}-{uid}.zip"
            )
            s3_service.store_stream(
                key,
                report_zip,
                content_type="application/zip",
            )

            s3_file_link = s3_service.generate_url(key)
            send_email(
                subject=f"Inventory and Holds Reports {current_time}",
                receivers=[email_address],
                text=(
                    f"Download Report here -> {s3_file_link} \n\n"
                    f"This report will be available for download for 30 days."
                ),
            )

            log.debug(f"Zip file written to {zip_path}")
            log.info(
                f"Emailed inventory and holds reports for {library.name}({library.short_name})."
            )


def create_temp_file() -> IO[str]:
    return tempfile.NamedTemporaryFile("w", encoding="utf-8")


def generate_csv_report(
    db: Session,
    csv_file: IO[str],
    sql_params: dict[str, Any],
    query: Select,
) -> None:
    with elapsed_time_logging(
        log_method=log.debug,
        message_prefix=f"generate_csv_report - {csv_file.name}",
        skip_start=True,
    ):
        writer = csv.writer(csv_file, delimiter=",")
        rows = db.execute(query, sql_params)
        writer.writerow(rows.keys())
        writer.writerows(rows)
        csv_file.flush()
        log.debug(f"report written to {csv_file.name}")


def _comma_separated_sorted_work_genre_list_subquery() -> Subquery:
    """Comma-separated list of genres for this work, in ascending order."""
    work_genre_alias = aliased(WorkGenre)
    genre_alias = aliased(Genre)
    return (
        select(
            work_genre_alias.work_id,
            func.array_to_string(
                func.array_agg(
                    aggregate_order_by(genre_alias.name, genre_alias.name.asc())
                ),
                ",",
            ).label("genres"),
        )
        .join(genre_alias, genre_alias.id == work_genre_alias.genre_id)
        .group_by(work_genre_alias.work_id)
        .subquery()
    )


def _best_isbn_lateral() -> Lateral:
    """Best ISBN for an identifier, if available."""
    id_isbn = aliased(Identifier)
    equivalency_alias = aliased(Equivalency)
    return lateral(
        select(id_isbn.identifier)
        .join(equivalency_alias, equivalency_alias.output_id == id_isbn.id)
        .where(
            equivalency_alias.input_id == Identifier.id,
            id_isbn.type == Identifier.ISBN,
            id_isbn.identifier.is_not(None),
            equivalency_alias.strength > 0.5,
            equivalency_alias.enabled == true(),
        )
        .order_by(equivalency_alias.strength.desc())
        .limit(1)
    )


def _library_holds_lateral() -> Lateral:
    """How many holds are active for this item in this library?"""
    hold_alias = aliased(Hold)
    patron_alias = aliased(Patron)
    return lateral(
        select(func.count(hold_alias.id).label("active_hold_count"))
        .join(patron_alias, hold_alias.patron_id == patron_alias.id)
        .where(
            hold_alias.license_pool_id == LicensePool.id,
            patron_alias.library_id == Library.id,
            (
                hold_alias.end.is_(None)
                | (hold_alias.end > func.now())
                | (hold_alias.position > 0)
            ),
        )
    )


def _is_shared_collection_lateral() -> Lateral:
    """Do other libraries share this collection?"""
    ilc_alias = aliased(IntegrationLibraryConfiguration)
    return lateral(
        select(
            (func.count(ilc_alias.parent_id) > 1).label("is_shared_collection")
        ).where(ilc_alias.parent_id == IntegrationConfiguration.id)
    )


def _licenses_lateral() -> Lateral:
    """License information, if present.

    Note that this may result in multiple rows.
    """
    license_alias = aliased(License)
    return lateral(
        select(license_alias).where(
            license_alias.license_pool_id == LicensePool.id,
        )
    )


def _library_loans_lateral() -> Lateral:
    """How many loans are active for this item in this library?"""
    loan_alias = aliased(Loan)
    patron_alias = aliased(Patron)
    return lateral(
        select(func.count(loan_alias.id).label("active_loan_count"))
        .join(patron_alias, loan_alias.patron_id == patron_alias.id)
        .where(
            loan_alias.license_pool_id == LicensePool.id,
            patron_alias.library_id == Library.id,
        )
    )


def inventory_report_query() -> Select:
    """A query for inventory report with license information.

    Note that this may result in multiple rows per license pool, if an item
    has more than one license.
    """

    isbn = _best_isbn_lateral()
    wg_subquery = _comma_separated_sorted_work_genre_list_subquery()
    collection_sharing = _is_shared_collection_lateral()
    lic = _licenses_lateral()

    return (
        select(
            lic.c.status,
            Edition.title,
            Edition.author,
            Identifier.identifier,
            func.coalesce(
                case(
                    (Identifier.type == Identifier.ISBN, Identifier.identifier),
                    else_=isbn.c.identifier,
                ),
                "",
            ).label("isbn"),
            Edition.language,
            Edition.publisher,
            Edition.medium.label("format"),
            Work.audience,
            func.coalesce(wg_subquery.c.genres, "").label("genres"),
            DataSource.name.label("data_source"),
            IntegrationConfiguration.name.label("collection_name"),
            lic.c.expires.label("license_expiration"),
            func.date_part("day", lic.c.expires - func.now()).label(
                "days_remaining_on_license"
            ),
            lic.c.checkouts_left.label("remaining_loans"),
            lic.c.terms_concurrency.label("allowed_concurrent_users"),
        )
        .select_from(LicensePool)
        .join(Identifier, LicensePool.identifier_id == Identifier.id)
        .outerjoin(isbn, Identifier.type != Identifier.ISBN)
        .join(Edition, Edition.id == LicensePool.presentation_edition_id)
        .join(Work, LicensePool.work_id == Work.id)
        .join(DataSource, LicensePool.data_source_id == DataSource.id)
        .join(Collection, LicensePool.collection_id == Collection.id)
        .join(
            IntegrationConfiguration,
            Collection.integration_configuration_id == IntegrationConfiguration.id,
        )
        .join(
            IntegrationLibraryConfiguration,
            IntegrationConfiguration.id == IntegrationLibraryConfiguration.parent_id,
        )
        .join(Library, IntegrationLibraryConfiguration.library_id == Library.id)
        .outerjoin(wg_subquery, Work.id == wg_subquery.c.work_id)
        .join(collection_sharing, true())
        .outerjoin(lic, true())
        .where(
            Library.id == bindparam("library_id"),
            IntegrationConfiguration.id.in_(
                bindparam("integration_ids", expanding=True)
            ),
        )
        .order_by(
            Edition.sort_title,
            Edition.sort_author,
            DataSource.name,
            IntegrationConfiguration.name,
        )
    )


def palace_inventory_activity_report_query() -> Select:
    """A query for the inventory activity report with loan and hold metrics."""

    isbn = _best_isbn_lateral()
    wg_subquery = _comma_separated_sorted_work_genre_list_subquery()
    collection_sharing = _is_shared_collection_lateral()
    lib_holds = _library_holds_lateral()
    lib_loans = _library_loans_lateral()

    return (
        select(
            Edition.title,
            Edition.author,
            Identifier.identifier,
            func.coalesce(
                case(
                    (Identifier.type == Identifier.ISBN, Identifier.identifier),
                    else_=isbn.c.identifier,
                ),
                "",
            ).label("isbn"),
            Edition.language,
            Edition.publisher,
            Edition.medium.label("format"),
            Work.audience,
            func.coalesce(wg_subquery.c.genres, "").label("genres"),
            DataSource.name.label("data_source"),
            IntegrationConfiguration.name.label("collection_name"),
            LicensePool.licenses_owned.label("total_library_allowed_concurrent_users"),
            func.coalesce(lib_loans.c.active_loan_count, 0).label(
                "library_active_loan_count"
            ),
            case(
                (
                    collection_sharing.c.is_shared_collection,
                    LicensePool.licenses_reserved,
                ),
                else_=-1,
            ).label("shared_active_loan_count"),
            func.coalesce(lib_holds.c.active_hold_count, 0).label(
                "library_active_hold_count"
            ),
            case(
                (
                    collection_sharing.c.is_shared_collection,
                    LicensePool.patrons_in_hold_queue,
                ),
                else_=-1,
            ).label("shared_active_hold_count"),
            case(
                (
                    LicensePool.licenses_owned > 0,
                    func.coalesce(lib_holds.c.active_hold_count, 0)
                    / LicensePool.licenses_owned,
                ),
                else_=-1,
            ).label("library_hold_ratio"),
        )
        .select_from(LicensePool)
        .join(Identifier, LicensePool.identifier_id == Identifier.id)
        .outerjoin(isbn, Identifier.type != Identifier.ISBN)
        .join(Edition, Edition.id == LicensePool.presentation_edition_id)
        .join(Work, LicensePool.work_id == Work.id)
        .join(DataSource, LicensePool.data_source_id == DataSource.id)
        .join(Collection, LicensePool.collection_id == Collection.id)
        .join(
            IntegrationConfiguration,
            Collection.integration_configuration_id == IntegrationConfiguration.id,
        )
        .join(
            IntegrationLibraryConfiguration,
            IntegrationConfiguration.id == IntegrationLibraryConfiguration.parent_id,
        )
        .join(Library, IntegrationLibraryConfiguration.library_id == Library.id)
        .outerjoin(wg_subquery, Work.id == wg_subquery.c.work_id)
        .outerjoin(lib_holds, true())
        .outerjoin(lib_loans, true())
        .join(collection_sharing, true())
        .where(
            Library.id == bindparam("library_id"),
            IntegrationConfiguration.id.in_(
                bindparam("integration_ids", expanding=True)
            ),
        )
        .order_by(
            Edition.sort_title,
            Edition.sort_author,
            DataSource.name,
            IntegrationConfiguration.name,
        )
    )


def holds_with_no_licenses_report_query() -> Select:
    """A query for holds report with hold information."""

    isbn = _best_isbn_lateral()
    wg_subquery = _comma_separated_sorted_work_genre_list_subquery()
    collection_sharing = _is_shared_collection_lateral()
    lib_holds = _library_holds_lateral()

    return (
        select(
            Edition.title,
            Edition.author,
            Identifier.identifier,
            func.coalesce(
                case(
                    (Identifier.type == Identifier.ISBN, Identifier.identifier),
                    else_=isbn.c.identifier,
                ),
                "",
            ).label("isbn"),
            Edition.language,
            Edition.publisher,
            Edition.medium.label("format"),
            Work.audience,
            func.coalesce(wg_subquery.c.genres, "").label("genres"),
            DataSource.name.label("data_source"),
            IntegrationConfiguration.name.label("collection_name"),
            func.coalesce(lib_holds.c.active_hold_count, 0).label(
                "library_active_hold_count"
            ),
            case(
                (
                    collection_sharing.c.is_shared_collection,
                    LicensePool.patrons_in_hold_queue,
                ),
                else_=-1,
            ).label("shared_active_hold_count"),
        )
        .select_from(LicensePool)
        .join(Identifier, LicensePool.identifier_id == Identifier.id)
        .outerjoin(isbn, Identifier.type != Identifier.ISBN)
        .join(Edition, Edition.id == LicensePool.presentation_edition_id)
        .join(Work, LicensePool.work_id == Work.id)
        .join(DataSource, LicensePool.data_source_id == DataSource.id)
        .join(Collection, LicensePool.collection_id == Collection.id)
        .join(
            IntegrationConfiguration,
            Collection.integration_configuration_id == IntegrationConfiguration.id,
        )
        .join(
            IntegrationLibraryConfiguration,
            IntegrationConfiguration.id == IntegrationLibraryConfiguration.parent_id,
        )
        .join(Library, IntegrationLibraryConfiguration.library_id == Library.id)
        .outerjoin(wg_subquery, Work.id == wg_subquery.c.work_id)
        .outerjoin(lib_holds, true())
        .join(collection_sharing, true())
        .where(
            Library.id == bindparam("library_id"),
            IntegrationConfiguration.id.in_(
                bindparam("integration_ids", expanding=True)
            ),
            LicensePool.licenses_owned == 0,
            # Only include items with holds in this library
            func.coalesce(lib_holds.c.active_hold_count, 0) > 0,
        )
        .order_by(
            Edition.sort_title,
            Edition.sort_author,
            DataSource.name,
            IntegrationConfiguration.name,
        )
    )


@shared_task(queue=QueueNames.high, bind=True)
def generate_inventory_and_hold_reports(
    task: Task, library_id: int, email_address: str
) -> None:
    with task.transaction() as session:
        generate_report(
            session,
            library_id=library_id,
            email_address=email_address,
            send_email=task.services.email.send_email,
            registry=task.services.integration_registry.license_providers(),
            s3_service=task.services.storage.public(),
        )
