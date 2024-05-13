from __future__ import annotations

import csv
import tempfile
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
from palace.manager.integration.registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.email.email import SendEmailCallable
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import get_one


def eligible_integrations(
    integrations: list[IntegrationConfiguration],
) -> list[IntegrationConfiguration]:
    """Subset a list of integrations to only those that are eligible for the inventory report."""
    registry = LicenseProvidersRegistry()

    def is_eligible(integration: IntegrationConfiguration) -> bool:
        if integration.protocol is None:
            return False
        settings = registry[integration.protocol].settings_load(integration)
        return isinstance(settings, OPDSImporterSettings)

    return [integration for integration in integrations if is_eligible(integration)]


def library_report_integrations(
    library: Library, session: Session | None = None
) -> list[IntegrationConfiguration]:
    """Return a list of collections to report for the given library."""
    if session is None:
        session = Session.object_session(library)

    # resolve integrations
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
    return sorted(eligible_integrations(integrations), key=lambda i: i.name)


class GenerateInventoryAndHoldsReportsJob(Job):
    def __init__(
        self,
        session_maker: sessionmaker[Session],
        library_id: int,
        email_address: str,
        send_email: SendEmailCallable,
        delete_attachments: bool = True,
    ):
        super().__init__(session_maker)
        self.library_id = library_id
        self.email_address = email_address
        self.delete_attachments = delete_attachments
        self.send_email = send_email

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
                    library=library, session=session
                )
            ]

            # generate inventory report csv file
            sql_params: dict[str, Any] = {
                "library_id": library.id,
                "integration_ids": tuple(integration_ids),
            }

            with tempfile.NamedTemporaryFile(
                delete=self.delete_attachments
            ) as report_zip:
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

                    self.send_email(
                        subject=f"Inventory and Holds Reports {current_time}",
                        receivers=[self.email_address],
                        text="",
                        attachments={
                            f"palace-inventory-and-holds-reports-for-{file_name_modifier}.zip": zip_path
                        },
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
            select
               e.title,
               e.author,
               i.identifier,
               e.language,
               e.publisher,
               e.medium as format,
               w.audience,
               wg.genres,
               d.name data_source,
               ic.name collection_name,
               l.expires license_expiration,
               DATE_PART('day', l.expires - now()) days_remaining_on_license,
               l.checkouts_left remaining_loans,
               l.terms_concurrency allowed_concurrent_users,
               coalesce(lib_loans.active_loan_count, 0) library_active_loan_count,
               CASE WHEN collection_sharing.is_shared_collection THEN lp.licenses_reserved
                    ELSE -1
               END shared_active_loan_count
        from datasources d,
             collections c,
             integration_configurations ic,
             integration_library_configurations il,
             libraries lib,
             works w left outer join (select wg.work_id, string_agg(g.name, ',' order by g.name) as genres
                                     from genres g,
                                     workgenres wg
                                     where g.id = wg.genre_id
                                     group by wg.work_id) wg on w.id = wg.work_id,
             editions e left outer join (select lp.presentation_edition_id,
                                         p.library_id,
                                         count(ln.id) active_loan_count
                                  from loans ln,
                                       licensepools lp,
                                       patrons p,
                                       libraries l
                                  where p.id = ln.patron_id and
                                        p.library_id = l.id and
                                        ln.license_pool_id = lp.id and
                                        l.id = :library_id
                                  group by p.library_id, lp.presentation_edition_id) lib_loans
                                  on e.id = lib_loans.presentation_edition_id,
             identifiers i,
             (select ilc.parent_id,
                      count(ilc.parent_id) > 1 is_shared_collection
              from integration_library_configurations ilc,
                   integration_configurations i,
                   collections c
              where c.integration_configuration_id = i.id  and
                    i.id = ilc.parent_id group by ilc.parent_id) collection_sharing,
             licensepools lp left outer join (select license_pool_id,
                                                     checkouts_left,
                                                     expires,
                                                     terms_concurrency
                                              from licenses where status = 'available') l on lp.id = l.license_pool_id
        where lp.identifier_id = i.id and
              e.primary_identifier_id = i.id and
              e.id = w.presentation_edition_id and
              d.id = e.data_source_id and
              c.id = lp.collection_id and
              c.integration_configuration_id = ic.id and
              ic.id = il.parent_id and
              ic.id = collection_sharing.parent_id and
              ic.id in :integration_ids and
              il.library_id = lib.id and
              lib.id = :library_id
         order by title, author
        """

    @staticmethod
    def holds_report_query() -> str:
        return """
            select
               e.title,
               e.author,
               i.identifier,
               e.language,
               e.publisher,
               e.medium as format,
               w.audience,
               wg.genres,
               d.name data_source,
               ic.name collection_name,
               coalesce(lib_holds.active_hold_count, 0) library_active_hold_count,
               CASE WHEN collection_sharing.is_shared_collection THEN lp.patrons_in_hold_queue
                    ELSE -1
               END shared_active_hold_count
        from datasources d,
             collections c,
             integration_configurations ic,
             integration_library_configurations il,
             libraries lib,
             works w left outer join (select wg.work_id, string_agg(g.name, ',' order by g.name) as genres
                                     from genres g,
                                     workgenres wg
                                     where g.id = wg.genre_id
                                     group by wg.work_id) wg on w.id = wg.work_id,
             editions e,
             (select lp.presentation_edition_id,
                                         p.library_id,
                                         count(h.id) active_hold_count
                                  from holds h,
                                       licensepools lp,
                                       patrons p
                                  where p.id = h.patron_id and
                                        h.license_pool_id = lp.id and
                                        p.library_id = :library_id and
                                        (h.end is null or
                                        h.end > now() or
                                        h.position > 0)
                                  group by p.library_id, lp.presentation_edition_id) lib_holds,
             identifiers i,
             (select ilc.parent_id,
                      count(ilc.parent_id) > 1 is_shared_collection
              from integration_library_configurations ilc,
                   integration_configurations i,
                   collections c
              where c.integration_configuration_id = i.id  and
                    i.id = ilc.parent_id group by ilc.parent_id) collection_sharing,
             licensepools lp
        where lp.identifier_id = i.id and
              e.primary_identifier_id = i.id and
              e.id = lib_holds.presentation_edition_id and
              e.id = w.presentation_edition_id and
              d.id = e.data_source_id and
              c.id = lp.collection_id and
              c.integration_configuration_id = ic.id and
              ic.id = il.parent_id and
              ic.id = collection_sharing.parent_id and
              ic.id in :integration_ids and
              il.library_id = lib.id and
              lib.id = :library_id
         order by title, author
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
    ).run()
