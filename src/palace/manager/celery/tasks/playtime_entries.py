from __future__ import annotations

import csv
import os
import random
import tempfile
import time
from collections import defaultdict
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from io import TextIOWrapper
from typing import TYPE_CHECKING, Any, Protocol

from celery import shared_task
from sqlalchemy import and_, distinct, false, select, true, union
from sqlalchemy.orm import Query, Session, joinedload
from sqlalchemy.sql.functions import coalesce, count, max as sql_max, sum

from palace.util.datetime_helpers import previous_months, utc_now

from palace.manager.api.config import Configuration
from palace.manager.celery.task import Task
from palace.manager.integration.license.opds.for_distributors.api import (
    OPDSForDistributorsAPI,
)
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.google_drive.google_drive import (
    GoogleDriveService,
    GoogleDriveUnavailable,
)
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.time_tracking import PlaytimeEntry, PlaytimeSummary

if TYPE_CHECKING:
    from googleapiclient._apis.drive.v3 import File


@shared_task(queue=QueueNames.default, bind=True)
def sum_playtime_entries(task: Task) -> None:
    """
    This task sums up unprocessed playtime entries, inserts them into the playtime_summaries table and removes
    older, processed playtime entries.
    """
    # Reap older processed entries
    older_than, _ = previous_months(number_of_months=1)
    older_than_ts = datetime(
        older_than.year, older_than.month, older_than.day, tzinfo=timezone.utc
    )

    with task.transaction() as tx:
        deleted = (
            tx.query(PlaytimeEntry)
            .filter(
                PlaytimeEntry.processed == true(),
                PlaytimeEntry.timestamp < older_than_ts,
            )
            .delete()
        )
        task.log.info(f"Deleted {deleted} entries. Older than {older_than_ts}")

        # Collect everything from one hour ago, reducing entries still in flux
        cut_off = utc_now() - timedelta(hours=1)

        # Fetch the unprocessed entries
        result = tx.query(PlaytimeEntry).filter(
            PlaytimeEntry.processed == false(),
            PlaytimeEntry.timestamp <= cut_off,
        )

        # Aggregate entries per identifier-timestamp-collection-library-loan_identifier grouping.
        # The label forms of the identifier, collection, and library are also
        # factored in, in case any of the foreign keys are missing.
        # Since timestamps should be on minute-boundaries the aggregation
        # can be written to PlaytimeSummary directly
        def group_key_for_entry(
            e: PlaytimeEntry,
        ) -> tuple[
            datetime,
            Identifier | None,
            Collection | None,
            Library | None,
            str,
            str,
            str,
            str,
            str,
        ]:
            return (
                e.timestamp,
                e.identifier,
                e.collection,
                e.library,
                e.identifier_str,
                e.collection_name,
                e.library_name,
                e.loan_identifier,
                e.data_source_name,
            )

        by_group: dict[Any, int] = defaultdict(int)
        for entry in result.all():
            by_group[group_key_for_entry(entry)] += entry.total_seconds_played
            entry.processed = True

        for group, seconds in by_group.items():
            # Values are in the same order returned from `group_key_for_entry` above.
            (
                timestamp,
                identifier,
                collection,
                library,
                identifier_str,
                collection_name,
                library_name,
                loan_identifier,
                data_source_name,
            ) = group

            # Update the playtime summary.
            playtime = PlaytimeSummary.add(
                tx,
                ts=timestamp,
                seconds=seconds,
                identifier=identifier,
                collection=collection,
                library=library,
                identifier_str=identifier_str,
                collection_name=collection_name,
                library_name=library_name,
                loan_identifier=loan_identifier,
                data_source_name=data_source_name,
            )
            task.log.info(
                f"Added {seconds} to {identifier_str} ({collection_name} in {library_name} with loan id of "
                f"{loan_identifier}) for {timestamp}: new total {playtime.total_seconds_played}."
            )


# TODO: Replace uses once we have a proper CSV writer type or protocol.
class Writer(Protocol):
    """CSV Writer protocol."""

    def writerow(self, row: Iterable[Any]) -> Any: ...


REPORT_DATE_FORMAT = "%m-%d-%Y"

# How long (seconds) to wait for the folder-creation lock before proceeding without it.
# Typed as float | int to match acquire_blocking's signature.
FOLDER_LOCK_ACQUIRE_TIMEOUT: float | int = 60

# Maximum seconds of random jitter added once per task invocation before the
# folder-creation loop. Independent CM groups share no Redis instance and therefore
# cannot coordinate via the lock alone; a single random offset per run spreads their
# arrival times across all folder levels without multiplying the delay by N data sources.
# Exposed as a module-level constant so tests can set it to 0.
FOLDER_CREATION_JITTER_MAX: int = 10

# How many times the task re-runs itself when Google Drive is unavailable, and
# the base delay (seconds) for the exponential backoff between those attempts.
# GoogleDriveService already retries within a single request, but that budget
# only spans ~30s; these cover a Drive outage lasting into the minutes. 4
# retries at a 60s base gives 60s, 120s, 240s and 480s -- a quarter of an hour
# in total.
#
# Those delays hold only because jitter is disabled on the task. Celery's
# retry_jitter defaults to True, which would pick each delay at random from
# between zero and the figures above: the schedule would cover about half as
# long on average, and the first retry could follow almost immediately. The
# floor matters as much as the total, because the delay is also what lets
# Drive's list API catch up with the previous attempt's writes before the skip
# check reads them. Spreading load across circulation managers is not a concern
# here -- that is what FOLDER_CREATION_JITTER_MAX does, on every run rather
# than only during an outage.
DRIVE_MAX_RETRIES = 4
DRIVE_RETRY_BACKOFF = 60


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=DRIVE_MAX_RETRIES,
    autoretry_for=(GoogleDriveUnavailable,),
    retry_backoff=DRIVE_RETRY_BACKOFF,
    retry_jitter=False,
)
def generate_playtime_report(
    task: Task,
    start: date | None = None,
    until: date | None = None,
) -> None:
    """
    This task generates a CSV report of playtime summaries based on a date range and uploads that report to
    a shared Google Drive. If no date range is supplied, it will include summaries from the previous month.

    The task is retried when Drive is unavailable. Because a retry re-runs the
    whole data source loop, each report's file name is determined entirely by
    the report's date range, data source and circulation manager, and a report
    already present in Drive is skipped -- so a retry resumes where the failed
    attempt stopped instead of uploading a second copy of everything that
    already succeeded.
    """

    default_start, default_until = (
        date for date in previous_months(number_of_months=1)
    )

    if not start:
        start = default_start

    if not until:
        until = default_until

    formatted_start_date = start.strftime(REPORT_DATE_FORMAT)
    formatted_until_date = until.strftime(REPORT_DATE_FORMAT)
    report_date_label = f"{formatted_start_date} - {formatted_until_date}"

    reporting_name = os.environ.get(
        Configuration.REPORTING_NAME_ENVIRONMENT_VARIABLE, ""
    )

    link_extension = "csv"

    google_drive_container = task.services.google_drive()
    google_drive = google_drive_container.service()

    # create directory hierarchy
    root_folder_id: str | None = google_drive_container.config.parent_folder_id()

    redis_client = task.services.redis.client()

    # Jitter: a single random delay per task invocation spreads independent CM groups
    # (which share no Redis instance) in time, reducing the probability that two groups
    # race to create the same Google Drive folder. Taking it once before the loop keeps
    # the per-data-source overhead constant regardless of how many sources are processed.
    time.sleep(random.uniform(0, FOLDER_CREATION_JITTER_MAX))

    with task.session() as session:
        # get list of collections
        data_source_names = _fetch_distinct_eligible_data_source_names(
            session=session,
            registry=task.services.integration_registry.license_providers(),
        )
        for data_source_name in data_source_names:
            reporting_name_with_no_spaces = (
                f"{reporting_name}-{data_source_name}".replace(" ", "_")
            )
            file_name_prefix = f"{formatted_start_date}-{formatted_until_date}-playtime-summary-{reporting_name_with_no_spaces}"
            linked_file_name = f"{file_name_prefix}.{link_extension}"

            nested_folders = [
                data_source_name,
                "Usage Reports",
                reporting_name,
                str(start.year),
            ]
            report_path = "/".join(nested_folders + [linked_file_name])

            with _drive_failures_naming(report_path):
                leaf_folder = _create_report_folders(
                    task=task,
                    google_drive=google_drive,
                    redis_client=redis_client,
                    nested_folders=nested_folders,
                    root_folder_id=root_folder_id,
                    data_source_name=data_source_name,
                )

                # Skip reports a previous attempt of this task already uploaded. The
                # folder hierarchy is created first so that this check can run before
                # the report is regenerated, which keeps a retry from repeating the
                # expensive query and CSV build for work that is already done.
                if google_drive.get_file(
                    name=linked_file_name, parent_folder_id=leaf_folder["id"]
                ):
                    task.log.info(
                        f"{report_path} already exists in "
                        "Google Drive; skipping. Remove the existing file if you need to "
                        "regenerate this report."
                    )
                    continue

                # Write to a temporary file so we don't overflow the memory
                with tempfile.NamedTemporaryFile(
                    "w+b",
                    prefix=f"{file_name_prefix}",
                    suffix=link_extension,
                ) as temp:
                    # Write the data as a CSV
                    writer = csv.writer(
                        TextIOWrapper(temp, encoding="utf-8", write_through=True)
                    )
                    _produce_report(
                        writer,
                        date_label=report_date_label,
                        records=_fetch_report_records(
                            session=session,
                            start=start,
                            until=until,
                            data_source_name=data_source_name,
                        ),
                    )

                    # store file
                    try:
                        google_drive.create_file(
                            file_name=linked_file_name,
                            parent_folder_id=leaf_folder["id"],
                            content_type="text/csv",
                            stream=temp,
                        )
                    except FileExistsError:
                        # The check above can miss a file this task uploaded moments
                        # earlier, because Drive's list API lags its writes. Losing
                        # that race means the report is already stored, which is the
                        # outcome we wanted, so treat it as success.
                        task.log.info(
                            f"{report_path} was already "
                            "present in Google Drive; skipping."
                        )
                        continue

                    task.log.info(
                        f"Stored {report_path} in Google Drive"
                        f"{'' if not root_folder_id else f' under the parent folder (id={root_folder_id}'}."
                    )


@contextmanager
def _drive_failures_naming(report_path: str) -> Generator[None]:
    """Name the report in any Drive failure raised inside the block.

    A run that exhausts its retries ends with this exception, and that message
    is all the operator gets to tell them which report was left unwritten. The
    service layer names the Drive call that failed but cannot know the report it
    belonged to, so the path is attached here, where it is.

    Re-raising the same type keeps the task's ``autoretry_for`` applicable, so
    adding the path costs no retries.

    :param report_path: The report's full path in Drive, folders included.
    """
    try:
        yield
    except GoogleDriveUnavailable as e:
        raise GoogleDriveUnavailable(
            f"Failed to store report {report_path}: {e}"
        ) from e


def _create_report_folders(
    *,
    task: Task,
    google_drive: GoogleDriveService,
    redis_client: Redis,
    nested_folders: list[str],
    root_folder_id: str | None,
    data_source_name: str,
) -> File:
    """
    Create (or look up) the folder hierarchy a report is stored in.

    :param task: The running task, used for logging.
    :param google_drive: The Drive service to create the folders with.
    :param redis_client: Redis client backing the folder-creation lock.
    :param nested_folders: Folder names, outermost first.
    :param root_folder_id: Drive folder/shared-drive ID to create them under.
    :param data_source_name: The data source, used to scope the lock.
    :return: The leaf folder, i.e. the one the report itself belongs in.
    """
    # Acquire a distributed lock so that concurrent workers within the
    # same Redis instance don't each create a duplicate folder hierarchy.
    # Google Drive allows same-named folders and has list-API indexing
    # latency, so a bare get-then-create is not safe under concurrent access.
    folder_lock = RedisLock(
        redis_client,
        lock_name=[
            "playtime_report_folder",
            root_folder_id or "default",
            data_source_name,
        ],
        lock_timeout=timedelta(minutes=5),
    )
    try:
        lock_acquired = folder_lock.acquire_blocking(
            timeout=FOLDER_LOCK_ACQUIRE_TIMEOUT
        )
    except Exception:
        # Redis may be unavailable (connection error, timeout, etc.).
        # Treat this the same as failing to acquire within the timeout:
        # log a warning and proceed without the lock rather than
        # aborting the entire report for this data source.
        lock_acquired = False
    if not lock_acquired:
        task.log.warning(
            f"Could not acquire folder creation lock for {data_source_name!r} "
            f"within {FOLDER_LOCK_ACQUIRE_TIMEOUT} seconds. Proceeding without "
            "lock — duplicate folders may be created."
        )
    try:
        folder_results = google_drive.create_nested_folders_if_not_exist(
            folders=nested_folders,
            parent_folder_id=root_folder_id,
        )
        # the leaf folder is the last path segment in the result list
        return folder_results[-1]
    finally:
        if lock_acquired:
            folder_lock.release()


def _fetch_distinct_eligible_data_source_names(
    session: Session,
    registry: LicenseProvidersRegistry,
) -> list[str]:
    """
    Fetches a sorted list of distinct data source names for which to produce a playtime report.

    Only OPDS 2.0 and OPDS for Distributors collections that have the
    ``generate_playtime_report`` setting explicitly set to ``True`` are included.
    All other collections are ignored.

    :param session: The SQLAlchemy database session.
    :param registry: The license providers registry for protocol lookups.
    :return: A sorted list of distinct data source names.
    """
    eligible_protocols = [OPDS2API, OPDSForDistributorsAPI]

    # Get IDs for all IntegrationConfiguration (canonical + aliases) for eligible integrations.
    eligible_config_ids_query = union(
        *[
            registry.configurations_query(protocol).with_only_columns(
                IntegrationConfiguration.id
            )
            for protocol in eligible_protocols
        ]
    )
    # Query collections with those configuration IDs, eagerly loading their configuration.
    eligible_collections_query = (
        select(Collection)
        .where(Collection.integration_configuration_id.in_(eligible_config_ids_query))
        .options(joinedload(Collection.integration_configuration))
    )
    eligible_collections = session.scalars(eligible_collections_query).all()

    # Only include collections that have opted in via the generate_playtime_report flag.
    # c.integration_configuration is guaranteed non-null by the query filter above.
    collection_ds_names = {
        c.data_source.name
        for c in eligible_collections
        if c.integration_configuration.settings_dict.get("generate_playtime_report")
        is True
        and c.data_source
        and c.data_source.name is not None
    }

    return sorted(collection_ds_names)


def _fetch_report_records(
    session: Session,
    start: date,
    until: date,
    data_source_name: str,
) -> Query[Any]:
    # The loan count query returns only non-empty string isbns and titles if there is more
    # than one row returned with the grouping.  This way we ensure that we do not
    # count the same loan twice in the case we have when a
    # 1. a single loan with identifier A
    # 2. and one or more playtime summaries with title A or no title or isbn A or no isbn
    # 3. and one more playtime summaries with title B, isbn B
    # This situation can occur when the title and isbn  metadata associated with an ID changes due to a feed
    # update that occurs between playlist entry posts.
    # in this case we just associate the loan identifier with one unique combination of the list of titles and isbn
    # values.
    loan_count_query = (
        select(
            PlaytimeSummary.identifier_str.label("identifier_str2"),
            PlaytimeSummary.collection_name.label("collection_name2"),
            PlaytimeSummary.library_name.label("library_name2"),
            sql_max(coalesce(PlaytimeSummary.isbn, "")).label("isbn2"),
            sql_max(coalesce(PlaytimeSummary.title, "")).label("title2"),
            count(distinct(PlaytimeSummary.loan_identifier)).label("loan_count"),
        )
        .where(
            and_(
                PlaytimeSummary.timestamp >= start,
                PlaytimeSummary.timestamp < until,
                PlaytimeSummary.data_source_name == data_source_name,
            )
        )
        .group_by(
            PlaytimeSummary.identifier_str,
            PlaytimeSummary.collection_name,
            PlaytimeSummary.library_name,
            PlaytimeSummary.identifier_id,
        )
        .subquery()
    )

    seconds_query = (
        select(
            PlaytimeSummary.identifier_str,
            PlaytimeSummary.collection_name,
            PlaytimeSummary.library_name,
            coalesce(PlaytimeSummary.isbn, "").label("isbn"),
            coalesce(PlaytimeSummary.title, "").label("title"),
            sum(PlaytimeSummary.total_seconds_played).label("total_seconds_played"),
        )
        .where(
            and_(
                PlaytimeSummary.timestamp >= start,
                PlaytimeSummary.timestamp < until,
                PlaytimeSummary.data_source_name == data_source_name,
            )
        )
        .group_by(
            PlaytimeSummary.identifier_str,
            PlaytimeSummary.collection_name,
            PlaytimeSummary.library_name,
            PlaytimeSummary.isbn,
            PlaytimeSummary.title,
            PlaytimeSummary.identifier_id,
        )
        .subquery()
    )

    combined = session.query(seconds_query, loan_count_query).outerjoin(
        loan_count_query,
        and_(
            seconds_query.c.identifier_str == loan_count_query.c.identifier_str2,
            seconds_query.c.collection_name == loan_count_query.c.collection_name2,
            seconds_query.c.library_name == loan_count_query.c.library_name2,
            seconds_query.c.isbn == loan_count_query.c.isbn2,
            seconds_query.c.title == loan_count_query.c.title2,
        ),
    )
    combined_sq = combined.subquery()

    return session.query(
        combined_sq.c.identifier_str,
        combined_sq.c.collection_name,
        combined_sq.c.library_name,
        combined_sq.c.isbn,
        combined_sq.c.title,
        combined_sq.c.total_seconds_played,
        coalesce(combined_sq.c.loan_count, 0),
    ).order_by(
        combined_sq.c.collection_name,
        combined_sq.c.library_name,
        combined_sq.c.identifier_str,
        combined_sq.c.title,
    )


def _produce_report(writer: Writer, date_label: str, records: Iterable[Any]) -> None:
    if not records:
        records = []
    writer.writerow(
        (
            "date",
            "urn",
            "isbn",
            "collection",
            "library",
            "title",
            "total seconds",
            "loan count",
        )
    )
    for (
        identifier_str,
        collection_name,
        library_name,
        isbn,
        title,
        total,
        loan_count,
    ) in records:
        row = (
            date_label,
            identifier_str,
            None if isbn == "" else isbn,
            collection_name,
            library_name,
            None if title == "" else title,
            total,
            loan_count,
        )
        # Write the row to the CSV
        writer.writerow(row)
