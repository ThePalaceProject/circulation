from __future__ import annotations

import csv
import tempfile
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import IO, Any

from celery import shared_task
from sqlalchemy import not_, select, text
from sqlalchemy.orm import Session, sessionmaker

from palace.manager.celery.job import Job
from palace.manager.celery.task import Task
from palace.manager.core.opds_import import OPDSImporterSettings
from palace.manager.integration.goals import Goals
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.email.email import SendEmailCallable
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.service.storage.s3 import S3Service
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import get_one
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


class GenerateInventoryAndHoldsReportsJob(Job):
    def __init__(
        self,
        session_maker: sessionmaker[Session],
        library_id: int,
        email_address: str,
        send_email: SendEmailCallable,
        registry: LicenseProvidersRegistry,
        s3_service: S3Service,
    ):
        super().__init__(session_maker)
        self.library_id = library_id
        self.email_address = email_address
        self.send_email = send_email
        self.registry = registry
        self.s3_service = s3_service

    def run(self) -> None:
        with self.transaction() as session:
            library = get_one(session, Library, id=self.library_id)

            if not library:
                self.log.error(
                    f"Cannot generate inventory and holds report for library (id={self.library_id}): "
                    f"library not found."
                )
                return

            self.log.info(
                f"Starting inventory and holds report job for {library.name}({library.short_name})."
            )

            current_time = datetime.now()
            date_str = current_time.strftime("%Y-%m-%d_%H:%M:%s")

            file_name_modifier = f"{library.short_name}-{date_str}"

            integration_ids = [
                integration.id
                for integration in library_report_integrations(
                    library=library, session=session, registry=self.registry
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
                    self.create_temp_file() as inventory_report_file,
                    self.create_temp_file() as holds_report_file,
                ):
                    self.generate_csv_report(
                        session,
                        csv_file=inventory_report_file,
                        sql_params=sql_params,
                        query=self.inventory_report_query(),
                    )

                    self.generate_csv_report(
                        session,
                        csv_file=holds_report_file,
                        sql_params=sql_params,
                        query=self.holds_report_query(),
                    )

                    with zipfile.ZipFile(
                        zip_path, "w", zipfile.ZIP_DEFLATED
                    ) as archive:
                        archive.write(
                            filename=holds_report_file.name,
                            arcname=f"palace-holds-report-for-library-{file_name_modifier}.csv",
                        )
                        archive.write(
                            filename=inventory_report_file.name,
                            arcname=f"palace-inventory-report-for-library-{file_name_modifier}.csv",
                        )

                    uid = uuid_encode(uuid.uuid4())
                    key = (
                        f"{S3Service.DOWNLOADS_PREFIX}/inventory_and_holds/{library.short_name}/"
                        f"inventory-and-holds-for-library-{file_name_modifier}-{uid}.zip"
                    )
                    self.s3_service.store_stream(
                        key,
                        report_zip,
                        content_type="application/zip",
                    )

                    s3_file_link = self.s3_service.generate_url(key)
                    self.send_email(
                        subject=f"Inventory and Holds Reports {current_time}",
                        receivers=[self.email_address],
                        text=(
                            f"Download Report here -> {s3_file_link} \n\n"
                            f"This report will be available for download for 30 days."
                        ),
                    )

                    self.log.debug(f"Zip file written to {zip_path}")
                    self.log.info(
                        f"Emailed inventory and holds reports for {library.name}({library.short_name})."
                    )

    def create_temp_file(self) -> IO[str]:
        return tempfile.NamedTemporaryFile("w", encoding="utf-8")

    def generate_csv_report(
        self,
        _db: Session,
        csv_file: IO[str],
        sql_params: dict[str, Any],
        query: str,
    ) -> None:
        writer = csv.writer(csv_file, delimiter=",")
        rows = _db.execute(
            text(query),
            sql_params,
        )
        writer.writerow(rows.keys())
        writer.writerows(rows)
        csv_file.flush()
        self.log.debug(f"report written to {csv_file.name}")

    @staticmethod
    def inventory_report_query() -> str:
        return """
           SELECT
                ed.title,
                ed.author,
                id.identifier,
                COALESCE(
                    CASE
                        WHEN id.type = 'ISBN' THEN id.identifier
                        ELSE isbn.identifier
                    END,
                    ''
                ) AS isbn,
                ed.language,
                ed.publisher,
                ed.medium AS format,
                w.audience,
                wg.genres,
                ds.name AS data_source,
                ic.name AS collection_name,
                lic.expires AS license_expiration,
                DATE_PART('day', lic.expires - NOW()) AS days_remaining_on_license,
                lic.checkouts_left AS remaining_loans,
                lic.terms_concurrency AS allowed_concurrent_users,
                COALESCE(lib_loans.active_loan_count, 0) AS library_active_loan_count,
                CASE
                    WHEN collection_sharing.is_shared_collection THEN lp.licenses_reserved
                    ELSE -1
                END AS shared_active_loan_count
            FROM licensepools lp
            JOIN identifiers id ON lp.identifier_id = id.id
            LEFT OUTER JOIN LATERAL (
                -- Best matching ISBN for this item, if available.
                -- Note: We do this only if primary identifier is not ISBN.
                SELECT isbn_sub.identifier
                FROM equivalents eq
                JOIN identifiers isbn_sub ON eq.output_id = isbn_sub.id
                WHERE eq.input_id = id.id
                  AND isbn_sub.type = 'ISBN' AND isbn_sub.identifier IS NOT NULL
                  AND eq.strength > 0.5 AND eq.enabled = true
                ORDER BY eq.strength DESC
                LIMIT 1
            ) isbn ON id.type != 'ISBN'
            JOIN editions ed ON ed.id = lp.presentation_edition_id
            JOIN works w ON lp.work_id = w.id
            JOIN datasources ds ON lp.data_source_id = ds.id
            JOIN collections c ON lp.collection_id = c.id
            JOIN integration_configurations ic ON c.integration_configuration_id = ic.id
            JOIN integration_library_configurations ilc ON ic.id = ilc.parent_id
            JOIN libraries lib ON ilc.library_id = lib.id
            LEFT OUTER JOIN (
                -- Comma-separated list of genres for this license pool's work.
                SELECT wg.work_id, STRING_AGG(g.name, ',' ORDER BY g.name) AS genres
                FROM genres g
                JOIN workgenres wg ON g.id = wg.genre_id
                GROUP BY wg.work_id
            ) wg ON w.id = wg.work_id
            LEFT OUTER JOIN LATERAL (
                -- How many loans are active for this item in this library?
                SELECT COUNT(ln.id) AS active_loan_count
                FROM loans ln
                JOIN patrons p ON ln.patron_id = p.id
                WHERE ln.license_pool_id = lp.id AND p.library_id = lib.id
            ) lib_loans ON TRUE
            JOIN LATERAL (
                -- Do other libraries share this collection?
                SELECT COUNT(ilc_sub.parent_id) > 1 AS is_shared_collection
                FROM integration_library_configurations ilc_sub
                WHERE ilc_sub.parent_id = ic.id
            ) collection_sharing ON TRUE
            LEFT OUTER JOIN LATERAL (
                -- License information, if present.
                -- Note that this may result in multiple rows.
                SELECT checkouts_left, expires, terms_concurrency
                FROM licenses
                WHERE license_pool_id = lp.id AND status = 'available'
            ) lic ON TRUE
            WHERE lib.id = :library_id AND ic.id IN :integration_ids
            ORDER BY ed.sort_title, ed.sort_author, ds.name, ic.name
        """

    @staticmethod
    def holds_report_query() -> str:
        return """
            SELECT
                ed.title,
                ed.author,
                id.identifier,
                COALESCE(
                    CASE
                        WHEN id.type = 'ISBN' THEN id.identifier
                        ELSE isbn.identifier
                    END,
                    ''
                ) AS isbn,
                ed.language,
                ed.publisher,
                ed.medium AS format,
                w.audience,
                wg.genres,
                d.name AS data_source,
                ic.name AS collection_name,
                COALESCE(lib_holds.active_hold_count, 0) AS library_active_hold_count,
                CASE
                    WHEN collection_sharing.is_shared_collection THEN lp.patrons_in_hold_queue
                    ELSE -1
                END AS shared_active_hold_count
            FROM licensepools lp
            JOIN identifiers id ON lp.identifier_id = id.id
            LEFT OUTER JOIN LATERAL (
                -- Best matching ISBN for this item, if available.
                -- Note: We do this only if primary identifier is not ISBN.
                SELECT isbn_sub.identifier
                FROM equivalents eq
                JOIN identifiers isbn_sub ON eq.output_id = isbn_sub.id
                WHERE eq.input_id = id.id
                  AND isbn_sub.type = 'ISBN' AND isbn_sub.identifier IS NOT NULL
                  AND eq.strength > 0.5 AND eq.enabled = true
                ORDER BY eq.strength DESC
                LIMIT 1
            ) isbn ON id.type != 'ISBN'
            JOIN editions ed ON ed.id = lp.presentation_edition_id
            JOIN works w ON lp.work_id = w.id
            JOIN datasources d ON lp.data_source_id = d.id
            JOIN collections c ON lp.collection_id = c.id
            JOIN integration_configurations ic ON c.integration_configuration_id = ic.id
            JOIN integration_library_configurations il ON ic.id = il.parent_id
            JOIN libraries lib ON il.library_id = lib.id
            LEFT OUTER JOIN (
                -- Comma-separated list of genres for this license pool's work.
                SELECT wg.work_id, STRING_AGG(g.name, ',' ORDER BY g.name) AS genres
                FROM genres g
                JOIN workgenres wg ON g.id = wg.genre_id
                GROUP BY wg.work_id
            ) wg ON w.id = wg.work_id
            LEFT OUTER JOIN LATERAL (
                -- How many holds are active for this item in this library?
                SELECT COUNT(h.id) AS active_hold_count
                FROM holds h
                JOIN patrons p ON h.patron_id = p.id
                WHERE h.license_pool_id = lp.id AND p.library_id = lib.id
                  AND (h.end IS NULL OR h.end > NOW() OR h.position > 0)
            ) lib_holds ON TRUE
           JOIN LATERAL (
                -- Do other libraries share this collection?
                SELECT COUNT(ilc.parent_id) > 1 AS is_shared_collection
                FROM integration_library_configurations ilc
                WHERE ilc.parent_id = ic.id
            ) collection_sharing ON TRUE
            WHERE lib.id = :library_id AND ic.id IN :integration_ids
            ORDER BY ed.sort_title, ed.sort_author, d.name, ic.name
        """


@shared_task(queue=QueueNames.high, bind=True)
def generate_inventory_and_hold_reports(
    task: Task, library_id: int, email_address: str
) -> None:
    GenerateInventoryAndHoldsReportsJob(
        task.session_maker,
        library_id=library_id,
        email_address=email_address,
        send_email=task.services.email.send_email,
        registry=task.services.integration_registry.license_providers(),
        s3_service=task.services.storage.public(),
    ).run()
