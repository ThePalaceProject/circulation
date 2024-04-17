from __future__ import annotations

import csv
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from celery import shared_task
from sqlalchemy import not_, select, text
from sqlalchemy.orm import Session, sessionmaker

from api.integration.registry.license_providers import LicenseProvidersRegistry
from core.celery.job import Job
from core.celery.task import Task
from core.exceptions import BasePalaceException
from core.integration.goals import Goals
from core.model import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
    Library,
    get_one,
)
from core.opds_import import OPDSImporterSettings
from core.service.celery.celery import QueueNames
from core.service.container import Services, container_instance


class GenerateInventoryAndHoldsReportsJob(Job):
    _delete_attachments = True

    def __init__(
        self, session_maker: sessionmaker[Session], library_id: int, email_address: str
    ):
        super().__init__(session_maker)
        self.library_id = library_id
        self.email_address = email_address

    @property
    def services(self) -> Services:
        return container_instance()

    def run(self) -> None:
        with self.transaction() as session:
            try:
                current_time = datetime.datetime.now()
                date_str = current_time.strftime("%Y-%m-%d_%H:%M:%s")
                attachments: dict[str, Path] = {}
                library = get_one(session, Library, id=self.library_id)

                if not library:
                    raise BasePalaceException(
                        message=f"Cannot generate inventory and holds report for library (id={self.library_id}):  "
                        f" library not found."
                    )

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

                attachments[f"palace-inventory-report-{file_name_modifier}.csv"] = Path(
                    inventory_report_file_path
                )
                attachments[f"palace-holds-report-{file_name_modifier}.csv"] = Path(
                    holds_report_file_path
                )

                self.services.email.send_email(
                    subject=f"Inventory and Holds Reports {current_time}",
                    receivers=[self.email_address],
                    text="",
                    attachments=attachments,
                )
            except Exception as e:
                # log error
                self.log.error(
                    f"Failed to send inventory and loans reports to "
                    f"{self.email_address} for {library.name} ({self.library_id})",
                    e,
                )
                return
            finally:
                if self._delete_attachments:
                    for file_path in attachments.values():
                        os.remove(file_path)

    def generate_inventory_report(self, _db, sql_params: dict[str, Any]) -> str:
        """Generate an inventory csv file and return the file path"""
        return self.generate_csv_report(_db, sql_params, self.inventory_report_query())

    def generate_holds_report(self, _db, sql_params: dict[str, Any]) -> str:
        """Generate a holds report csv file and return the file path"""
        return self.generate_csv_report(_db, sql_params, self.holds_report_query())

    def generate_csv_report(self, _db, sql_params: dict[str, Any], query: str):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as temp:
            writer = csv.writer(temp, delimiter=",")
            rows = _db.execute(
                text(query),
                sql_params,
            )
            writer.writerow(rows.keys())
            writer.writerows(rows)
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
        task.session_maker, library_id=library_id, email_address=email_address
    ).run()
