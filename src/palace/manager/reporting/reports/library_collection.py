from __future__ import annotations

import tempfile
import zipfile
from collections.abc import Sequence
from datetime import datetime
from functools import cached_property, partial
from pathlib import Path
from typing import IO, ClassVar, TypedDict, TypeVar

from sqlalchemy.orm import Session
from typing_extensions import Unpack

from palace.manager.celery.task import Task
from palace.manager.reporting.model import ReportTable
from palace.manager.reporting.util import (
    RequestIdLoggerAdapter,
    row_counter_wrapper,
    write_csv,
)
from palace.manager.service.email.email import SendEmailCallable
from palace.manager.service.storage.s3 import S3Service
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.log import LoggerAdapterType, LoggerMixin


class LibraryReportKwargs(TypedDict, total=False):
    """Keyword arguments for the library report generator task."""

    request_id: str
    email_address: str
    library_id: int
    collection_ids: Sequence[int] | None


T = TypeVar("T", bound="LibraryCollectionReport")


class LibraryCollectionReport(LoggerMixin):
    TIMESTAMP_FORMAT_FOR_FILENAMES = "%Y-%m-%dT%H-%M-%S"
    TIMESTAMP_FORMAT_FOR_EMAILS = "%Y-%m-%d %H:%M:%S"
    # The following must be defined in subclasses.
    KEY: ClassVar[str]
    TITLE: ClassVar[str]
    TABLE_CLASSES: ClassVar[list[type[ReportTable]]] = []

    @property
    def key(self) -> str:
        return self.KEY

    @property
    def title(self) -> str:
        return self.TITLE

    @property
    def table_classes(self) -> list[type[ReportTable]]:
        if self.TABLE_CLASSES is None or len(self.TABLE_CLASSES) == 0:
            raise ValueError(
                f"No table classes defined for report '{self.title}' ({self.key})."
            )
        return self.TABLE_CLASSES

    @classmethod
    def from_task(cls: type[T], task: Task, **kwargs: Unpack[LibraryReportKwargs]) -> T:
        return cls(
            send_email=task.services.email.send_email,
            s3_service=task.services.storage.public(),
            **kwargs,
        )

    def __init__(
        self,
        *,
        send_email: SendEmailCallable,
        s3_service: S3Service,
        request_id: str,
        library_id: int,
        collection_ids: Sequence[int] | None = None,
        email_address: str,
    ) -> None:
        super().__init__()
        self.request_id = request_id
        self.library_id = library_id
        self.collection_ids = collection_ids
        self.email_address = email_address
        self.store_s3_stream = s3_service.store_stream
        self.send_email = send_email
        self._timestamp: datetime | None = None
        self._library: Library | None = None

    @cached_property
    def log(self) -> LoggerAdapterType:
        """Return a specialized logger for this class."""
        return RequestIdLoggerAdapter(self.logger(), {"id": self.request_id})

    @property
    def library(self) -> Library:
        """Return the library for this report."""
        if self._library is None:
            raise ValueError(f"Library not set for report '{self.title}' ({self.key}).")
        return self._library

    @property
    def timestamp(self) -> datetime:
        """Return the datetime for this report"""
        if self._timestamp is None:
            raise ValueError(
                f"Timestamp not set for report '{self.title}' ({self.key}). "
                "It should be set in the `run` method to capture the timestamp when the report is run."
            )
        return self._timestamp

    @classmethod
    def eligible_integrations(cls, library: Library) -> list[IntegrationConfiguration]:
        """Return the "eligible" IntegrationConfigurations for the given Library."""
        return [c.integration_configuration for c in library.active_collections]

    def timestamp_filename_string(self) -> str:
        return self.timestamp.strftime(self.TIMESTAMP_FORMAT_FOR_FILENAMES)

    def timestamp_email_string(self) -> str:
        return self.timestamp.strftime(self.TIMESTAMP_FORMAT_FOR_EMAILS)

    def get_filename(self, key: str | None = None) -> str:
        _key: str = key if key is not None else self.key
        date_str = self.timestamp_filename_string()
        return f"palace-{_key}-{self.library.short_name}-{date_str}"

    def email_subject(self, library_name: str | None = None) -> str:
        if library_name is None:
            library_name = self.library.name
        return (
            f"Palace report '{self.title}' for library '{library_name}' run at {self.timestamp_email_string()} "
            f"(request id: {self.request_id})"
        )

    def send_success_notification(self, *, download_url: str) -> None:
        self.send_email(
            receivers=self.email_address,
            subject=self.email_subject(),
            text=(
                f"Download report here -> {download_url} \n\n"
                f"This report will be available to download for 30 days."
            ),
        )

    def send_error_notification(
        self, *, subject: str | None = None, library_name: str | None = None
    ) -> None:
        _library_name = (
            library_name
            if library_name is not None
            else (self.library.name if self._library else "an unknown library")
        )
        _subject = (
            subject
            if subject is not None
            else self.email_subject(library_name=_library_name)
        )
        self.log.error(
            f"Error generating report '{self.title}' ({self.key}) for library {_library_name}."
        )
        self.send_email(
            receivers=self.email_address,
            subject=_subject,
            text=(
                f"There was an error generating the '{self.title}' report for {_library_name}. \n\n"
                "If the issue persists, please contact support."
            ),
        )

    def store_to_s3(
        self, *, file: IO[bytes], name: str, extension: str = "", content_type: str
    ) -> str | None:
        """Store content to S3.

        The name and extension are used to construct a key for the S3
        object. A request ID is injected into the key to avoid creating
        predictable S3 URLs.

        :param file: A file-like object with the contents to store.
        :param name: The name for the S3 key.
        :param extension: The extension, including the dot('.'), for the S3 key.
        :param content_type: The MIME type for the stored object.
        :return: The URL for the stored object on success or None on failure.
        """
        key = (
            f"{S3Service.DOWNLOADS_PREFIX}/reports/{name}-{self.request_id}{extension}"
        )
        return self.store_s3_stream(
            key,
            file,
            content_type=content_type,
        )

    def zip_results(
        self,
        *,
        archive: zipfile.ZipFile,
        member_name: str,
        table: ReportTable,
    ) -> None:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", newline=""
        ) as temp_file:
            # Generate the report.
            csv_file_writer = partial(write_csv, file=temp_file, delimiter=",")
            processor = row_counter_wrapper(csv_file_writer)
            counted_rows, _ = table(processor)
            self.log.debug(
                f"Wrote {counted_rows.get_count()} rows to file {temp_file.name}."
            )

            # Put it in the Zip file.
            archive.write(
                filename=temp_file.name,
                arcname=member_name,
            )
            self.log.debug(
                f"Report file added to Zip archive '{archive.filename}' as '{member_name}'."
            )

    def _run_report(self) -> bool:
        """Run the report for the given library."""

        library = self.library
        report_filename = self.get_filename(key=self.key)
        self.log.info(
            f"Creating report '{self.key}' for {library.name} ({library.short_name})."
        )

        # Instantiate the table objects for this report.
        tables = [
            t(library=library, collection_ids=self.collection_ids)
            for t in self.table_classes
        ]

        with tempfile.NamedTemporaryFile() as temp_zip_file:
            zip_path = Path(temp_zip_file.name)

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as archive:
                for table in tables:
                    self.zip_results(
                        archive=archive,
                        member_name=f"{self.get_filename(key=table.definition.key)}.csv",
                        table=table,
                    )
            self.log.debug(f"Zip file written to '{zip_path}'.")

            # This step must be done after the Zip `archive` has been closed,
            # but before it's temporary file has been deleted. `archive` is
            # automatically closed when its context manager exits, so this step
            # should happen outside of that context manager or after `archive.close()`
            # has been called. The temporary file will be deleted when its context
            # manager exits, so this step must be performed within that context manager.
            # Store the Zip to S3.
            s3_url = self.store_to_s3(
                file=temp_zip_file,
                name=f"{library.short_name}/{report_filename}",
                extension=".zip",
                content_type="application/zip",
            )
            if s3_url is None:
                self.log.error(
                    f"Failed to store report '{self.key}' for {library.name} ({library.short_name}) to S3."
                )
                self.send_error_notification()
                return False

        # Notify the requestor.
        self.send_success_notification(download_url=s3_url)
        self.log.info(
            f"Emailed notification for report '{self.title}' ({self.key}) for "
            f"library {library.name} ({library.short_name}) to {self.email_address}."
        )

        return True

    def run(self, *, session: Session) -> bool:
        """Run the main report task with a database session."""

        # Set the timestamp for the current run.
        self._timestamp = datetime.now()

        library = get_one(session, Library, id=self.library_id)
        if not library:
            self.log.error(
                f"Unable to generate report '{self.title}' ({self.key}) for library id={self.library_id}: "
                "library not found."
            )
            self.send_error_notification(
                subject=self.email_subject(library_name="an unknown library")
            )
            return False
        self._library = library
        self.log.info(
            f"Set library '{self.library.name}' ({self.library.short_name}) for report '{self.title}' ({self.key})."
        )

        return self._run_report()
