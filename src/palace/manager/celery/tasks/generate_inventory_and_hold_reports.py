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
                        report_zip,  # type: ignore[arg-type]
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
                e.title,
                e.author,
                i.identifier,
                COALESCE(
                    CASE
                        WHEN i.type = 'ISBN' THEN i.identifier
                        ELSE isbn.identifier
                    END,
                    ''
                ) AS isbn,
                e.language,
                e.publisher,
                e.medium AS format,
                w.audience,
                wg.genres,
                d.name AS data_source,
                ic.name AS collection_name,
                l.expires AS license_expiration,
                DATE_PART('day', l.expires - NOW()) AS days_remaining_on_license,
                l.checkouts_left AS remaining_loans,
                l.terms_concurrency AS allowed_concurrent_users,
                COALESCE(lib_loans.active_loan_count, 0) AS library_active_loan_count,
                CASE
                    WHEN collection_sharing.is_shared_collection THEN lp.licenses_reserved
                    ELSE -1
                END AS shared_active_loan_count
            FROM licensepools lp
            JOIN identifiers i ON lp.identifier_id = i.id
            LEFT OUTER JOIN (
                SELECT DISTINCT ON (eq.input_id) eq.input_id, isbn.identifier
                FROM equivalents eq
                JOIN identifiers isbn ON eq.output_id = isbn.id
                WHERE isbn.type = 'ISBN' AND isbn.identifier IS NOT NULL
                ORDER BY eq.input_id, eq.strength DESC
            ) isbn ON i.type != 'ISBN' AND i.id = isbn.input_id
            JOIN editions e ON e.primary_identifier_id = i.id
            JOIN works w ON lp.work_id = w.id
            JOIN datasources d ON e.data_source_id = d.id
            JOIN collections c ON lp.collection_id = c.id
            JOIN integration_configurations ic ON c.integration_configuration_id = ic.id
            JOIN integration_library_configurations il ON ic.id = il.parent_id
            JOIN libraries lib ON il.library_id = lib.id
            LEFT JOIN (
                SELECT wg.work_id, STRING_AGG(g.name, ',' ORDER BY g.name) AS genres
                FROM genres g
                JOIN workgenres wg ON g.id = wg.genre_id
                GROUP BY wg.work_id
            ) wg ON w.id = wg.work_id
            LEFT JOIN (
                SELECT lp.presentation_edition_id, p.library_id, COUNT(ln.id) AS active_loan_count
                FROM loans ln
                JOIN licensepools lp ON ln.license_pool_id = lp.id
                JOIN patrons p ON ln.patron_id = p.id
                JOIN libraries l ON p.library_id = l.id
                WHERE l.id = :library_id
                GROUP BY p.library_id, lp.presentation_edition_id
            ) lib_loans ON e.id = lib_loans.presentation_edition_id
            JOIN (
                SELECT ilc.parent_id, COUNT(ilc.parent_id) > 1 AS is_shared_collection
                FROM integration_library_configurations ilc
                JOIN integration_configurations i ON ilc.parent_id = i.id
                JOIN collections c ON i.id = c.integration_configuration_id
                GROUP BY ilc.parent_id
            ) collection_sharing ON ic.id = collection_sharing.parent_id
            LEFT JOIN (
                SELECT license_pool_id, checkouts_left, expires, terms_concurrency
                FROM licenses
                WHERE status = 'available'
            ) l ON lp.id = l.license_pool_id
            WHERE ic.id IN :integration_ids AND lib.id = :library_id
            ORDER BY e.title, e.author
        """

    @staticmethod
    def holds_report_query() -> str:
        return """
            SELECT
                e.title,
                e.author,
                i.identifier,
                COALESCE(
                    CASE
                        WHEN i.type = 'ISBN' THEN i.identifier
                        ELSE isbn.identifier
                    END,
                    ''
                ) AS isbn,
                e.language,
                e.publisher,
                e.medium AS format,
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
            JOIN identifiers i ON lp.identifier_id = i.id
            LEFT OUTER JOIN (
                SELECT DISTINCT ON (eq.input_id) eq.input_id, isbn.identifier
                FROM equivalents eq
                JOIN identifiers isbn ON eq.output_id = isbn.id
                WHERE isbn.type = 'ISBN' AND isbn.identifier IS NOT NULL
                ORDER BY eq.input_id, eq.strength DESC
            ) isbn ON i.type != 'ISBN' AND i.id = isbn.input_id
            JOIN editions e ON e.primary_identifier_id = i.id
            JOIN works w ON lp.work_id = w.id
            JOIN datasources d ON e.data_source_id = d.id
            JOIN collections c ON lp.collection_id = c.id
            JOIN integration_configurations ic ON c.integration_configuration_id = ic.id
            JOIN integration_library_configurations il ON ic.id = il.parent_id
            JOIN libraries lib ON il.library_id = lib.id
            LEFT OUTER JOIN (
                SELECT wg.work_id, STRING_AGG(g.name, ',' ORDER BY g.name) AS genres
                FROM genres g
                JOIN workgenres wg ON g.id = wg.genre_id
                GROUP BY wg.work_id
            ) wg ON w.id = wg.work_id
            JOIN (
                SELECT lp.presentation_edition_id,  p.library_id, COUNT(h.id) AS active_hold_count
                FROM holds h
                JOIN licensepools lp ON h.license_pool_id = lp.id
                JOIN patrons p ON h.patron_id = p.id
                WHERE p.library_id = :library_id AND (h.end IS NULL OR h.end > NOW() OR h.position > 0)
                GROUP BY p.library_id, lp.presentation_edition_id
            ) lib_holds ON e.id = lib_holds.presentation_edition_id
            JOIN (
                SELECT ilc.parent_id, COUNT(ilc.parent_id) > 1 AS is_shared_collection
                FROM integration_library_configurations ilc
                JOIN integration_configurations i ON ilc.parent_id = i.id
                JOIN collections c ON i.id = c.integration_configuration_id
                GROUP BY ilc.parent_id
            ) collection_sharing ON ic.id = collection_sharing.parent_id
            WHERE ic.id IN :integration_ids AND lib.id = :library_id
            ORDER BY e.title, e.author
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
