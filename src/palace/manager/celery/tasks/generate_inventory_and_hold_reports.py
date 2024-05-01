from __future__ import annotations

import csv
import os
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

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

            try:
                current_time = datetime.now()
                date_str = current_time.strftime("%Y-%m-%d_%H:%M:%s")
                attachments: dict[str, Path] = {}

                file_name_modifier = f"{library.short_name}-{date_str}"

                # resolve integrations
                integrations = session.scalars(
                    select(IntegrationConfiguration)
                    .join(IntegrationLibraryConfiguration)
                    .where(
                        IntegrationLibraryConfiguration.library_id == self.library_id,
                        IntegrationConfiguration.goal == Goals.LICENSE_GOAL,
                        not_(
                            IntegrationConfiguration.settings_dict.contains(
                                {"include_in_inventory_report": False}
                            )
                        ),
                    )
                ).all()
                registry = LicenseProvidersRegistry()
                integration_ids: list[int] = []
                for integration in integrations:
                    settings = registry[integration.protocol].settings_load(integration)
                    if not isinstance(settings, OPDSImporterSettings):
                        continue
                    integration_ids.append(integration.id)

                # generate inventory report csv file
                sql_params: dict[str, Any] = {
                    "library_id": library.id,
                    "integration_ids": tuple(integration_ids),
                }

                inventory_report_file_path = self.generate_inventory_report(
                    session, sql_params=sql_params
                )

                # generate holds report csv file
                holds_report_file_path = self.generate_holds_report(
                    session, sql_params=sql_params
                )

                with tempfile.NamedTemporaryFile(delete=False) as tmp:
                    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as archive:
                        archive.write(
                            filename=holds_report_file_path,
                            arcname=f"palace-holds-report-for-library-{file_name_modifier}.csv",
                        )
                        archive.write(
                            filename=inventory_report_file_path,
                            arcname=f"palace-inventory-report-for-library-{file_name_modifier}.csv",
                        )

                self.log.debug(f"Zip file written to {tmp.name}")
                # clean up report files now that they have been written to the zipfile
                for f in [inventory_report_file_path, holds_report_file_path]:
                    os.remove(f)

                attachments[
                    f"palace-inventory-and-holds-reports-for-{file_name_modifier}.zip"
                ] = Path(tmp.name)
                self.send_email(
                    subject=f"Inventory and Holds Reports {current_time}",
                    receivers=[self.email_address],
                    text="",
                    attachments=attachments,
                )

                self.log.info(
                    f"Emailed inventory and holds reports for {library.name}({library.short_name})."
                )
            finally:
                if self.delete_attachments:
                    for file_path in attachments.values():
                        os.remove(file_path)

    def generate_inventory_report(
        self, _db: Session, sql_params: dict[str, Any]
    ) -> str:
        """Generate an inventory csv file and return the file path"""
        return self.generate_csv_report(_db, sql_params, self.inventory_report_query())

    def generate_holds_report(self, _db: Session, sql_params: dict[str, Any]) -> str:
        """Generate a holds report csv file and return the file path"""
        return self.generate_csv_report(_db, sql_params, self.holds_report_query())

    def generate_csv_report(
        self, _db: Session, sql_params: dict[str, Any], query: str
    ) -> str:
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as temp:
            writer = csv.writer(temp, delimiter=",")
            rows = _db.execute(
                text(query),
                sql_params,
            )
            writer.writerow(rows.keys())
            writer.writerows(rows)

        self.log.debug(f"temp file written to {temp.name}")
        return temp.name

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
