from __future__ import annotations

import tempfile
import zipfile
from collections.abc import Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property, partial
from io import TextIOWrapper
from typing import IO, ClassVar, TypedDict, TypeVar

from sqlalchemy.orm import Session
from typing_extensions import Unpack

from palace.manager.celery.task import Task
from palace.manager.core.exceptions import IntegrationException
from palace.manager.reporting.model import ReportTable, TTabularDataProcessor
from palace.manager.reporting.tables.library_all_title import LibraryAllTitleReportTable
from palace.manager.reporting.util import (
    RequestIdLoggerAdapter,
    TimestampFormat,
    row_counter_wrapper,
    write_csv,
)
from palace.manager.service.email.email import SendEmailCallable
from palace.manager.service.storage.s3 import S3Service
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.log import LoggerAdapterType, LoggerMixin


class LibraryReportKwargs(TypedDict, total=False):
    """Keyword arguments for the library report generator task."""

    request_id: str
    email_address: str
    library_id: int
    collection_ids: Sequence[int] | None


@dataclass
class TableProcessingResult:
    """Result of processing a single table."""

    key: str
    title: str
    filename: str
    row_count: int
    content_stream: IO[bytes]


T = TypeVar("T", bound="LibraryCollectionReport")


class LibraryCollectionReport(LoggerMixin):
    # The following must be defined in subclasses:
    #   A unique key for this report.
    KEY: ClassVar[str]
    #   A human-readable title for this report.
    TITLE: ClassVar[str]
    #   A list of the classes for the tables that constitute this report.
    #   Note: A report may consist of more than one table.
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
    def eligible_collections(cls, library: Library) -> list[Collection]:
        """Return the "eligible" Collections for the given Library."""
        return list(library.active_collections)

    def timestamp_email_string(self) -> str:
        return TimestampFormat.EMAIL.format_timestamp(self.timestamp)

    def get_filename(self, key: str | None = None) -> str:
        _key: str = key if key is not None else self.key
        date_str = TimestampFormat.FILENAME.format_timestamp(self.timestamp)
        return f"palace-{_key}-{self.library.short_name}-{date_str}"

    def email_subject(self, library_text: str | None = None) -> str:
        if library_text is None:
            library_text = f"library '{self.library.name}'"
        runtime = TimestampFormat.EMAIL.format_timestamp(self.timestamp)
        return (
            f"Palace report '{self.title}' for {library_text} run at {runtime} "
            f"(request id: {self.request_id})"
        )

    def send_success_notification(self, *, access_url: str) -> None:
        self.send_email(
            receivers=self.email_address,
            subject=self.email_subject(),
            text=(
                f"You may access the report here -> {access_url} \n\n"
                f"This report will be available to download for 30 days."
            ),
        )

    def send_error_notification(self, *, library_name: str | None = None) -> None:
        library_name_ = (
            library_name
            if library_name is not None
            else (self.library.name if self._library else None)
        )
        library_text = (
            f"library '{library_name_}'" if library_name_ else "an unknown library"
        )
        self.send_email(
            receivers=self.email_address,
            subject=self.email_subject(library_text=library_text),
            text=(
                f"There was an error generating the '{self.title}' report for {library_text}. \n\n"
                "If the issue persists, please contact support."
            ),
        )

    def get_table_processor(
        self, table: ReportTable, **kwargs
    ) -> TTabularDataProcessor:
        """Get the processor for a specific table.

        Override in subclasses for different processing strategies (e.g.,
        non-CSV or different processors for different tables).

        Note: This looks like a static method, but it is not. As a hook, it
        may need information about the table being processed or information
        from the class or instance.

        :param table: The table for which to get a processor.
        :return: A tabular data processor for the table.
        """
        delimiter = kwargs.pop("delimiter", ",")
        return partial(write_csv, delimiter=delimiter, **kwargs)

    def _process_table(
        self, table: ReportTable, *, stack: ExitStack
    ) -> TableProcessingResult:
        """A simple wrapper around _process_table that handles exceptions and logs them properly."""
        try:
            result = self._process_table_base(table, stack=stack)
        except Exception as e:
            self.log.exception(f"Failed to process table '{table.definition.key}': {e}")
            raise

        self.log.debug(
            f"Processed table '{table.definition.title}' ({table.definition.key}) "
            f"({result.row_count} rows) "
            f"to '{result.filename}' for report '{self.title}' ({self.key})"
        )

        return result

    def _process_table_base(
        self, table: ReportTable, *, stack: ExitStack
    ) -> TableProcessingResult:
        """Process a single table and return the result with metadata.

        :param table: A ReportTable instance.
        :param stack: A `contextlib.ExitStack`.
        :return: A TableProcessingResult object with table metadata and output stream.
        :raises: Any exception raised during table processing.
        """
        filename = f"{self.get_filename(key=table.definition.key)}.csv"

        # Create a single temporary file opened in text mode
        binary_file = stack.enter_context(tempfile.NamedTemporaryFile("w+b"))

        # Get a text mode file object to write the table output into.
        output_file = TextIOWrapper(binary_file, encoding="utf-8", newline="")

        # Get the table processor bound to the output file.
        processor_func = self.get_table_processor(table, file=output_file)
        counting_processor = row_counter_wrapper(processor_func)

        # Process the table
        counted_rows, _ = table(counting_processor)

        # Flush the output file to ensure all data is written.
        output_file.flush()
        output_file.detach()

        # Rewind the file so that it's ready for reading.
        binary_file.seek(0)

        # Return table processing result metadata and content stream.
        return TableProcessingResult(
            key=table.definition.key,
            title=table.definition.title,
            filename=filename,
            row_count=counted_rows.get_count(),
            content_stream=binary_file,
        )

    def _package_results(
        self, results: list[TableProcessingResult], *, stack: ExitStack
    ) -> IO[bytes]:
        """Package processing results into a ZIP archive.

        Override in subclasses, depending on the packaging approach.

        This is a good place to apply a consistent process to each table's results.

        :param results: List of table processing results to package.
        :param stack: A `contextlib.ExitStack`.
        :return: A stream containing the packaged results.
        :raises: Any exception raised during packaging.
        """
        zip_buffer = stack.enter_context(
            tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024)
        )

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as archive:
            for result in results:
                # Read content and add to ZIP
                content = result.content_stream.read()
                archive.writestr(result.filename, content)

                self.log.debug(
                    f"Added {result.filename} ({result.row_count} rows) to ZIP archive."
                )

        zip_buffer.seek(0)
        return zip_buffer

    def _store_package(self, package: IO[bytes]) -> str:
        """Store the packaged results to S3.

        The request ID is used to obfuscate the URL.

        Override in subclasses for different storage strategies.

        :param package: The packaged results to store.
        :return: Access URL for the stored results.
        :raises: IntegrationException if the storage operation fails.
        """
        library = self.library

        # Construct S3 key with request ID for obfuscation.
        key = (
            f"{S3Service.DOWNLOADS_PREFIX}/reports/"
            f"{library.short_name}/{self.get_filename()}-{self.request_id}.zip"
        )

        result = self.store_s3_stream(
            key,
            package,
            content_type="application/zip",
        )
        if result is None:
            message = f"Failed to store report '{self.key}' for library '{library.name}' ({library.short_name}) to S3."
            self.log.error(message)
            raise IntegrationException(message)
        return result

    def _initialize_tables(self, session: Session) -> list[ReportTable]:
        """Prepare table instances for the report.

        :param session: Database session.
        :return: List of instantiated table objects.
        """
        return [
            t(
                session=session,
                library_id=self.library.id,
                collection_ids=self.collection_ids,
            )
            for t in self.table_classes
        ]

    def _run_report(self, *, session: Session) -> bool:
        """Run the report for the given library."""
        library = self.library
        self.log.info(
            f"Creating report '{self.key}' for {library.name} ({library.short_name})."
        )

        # Set up the tables.
        tables = self._initialize_tables(session=session)

        with ExitStack() as stack:
            # Process each table and capture the results.
            processing_results = [
                self._process_table(table, stack=stack) for table in tables
            ]

            # Package up the results.
            try:
                package = self._package_results(processing_results, stack=stack)
            except Exception as e:
                self.log.exception(f"Failed to package results: {e}")
                raise

            # Store the package.
            location = self._store_package(package)

        # Send success notification.
        self.send_success_notification(access_url=location)
        self.log.info(
            f"Completed report '{self.key}' for {library.name} ({library.short_name})."
        )
        return True

    def run(self, *, session: Session) -> bool:
        """Set up and run the report.

        :param session: Database session.
        :return: True if the report was successfully generated, False otherwise.
        """

        # Set the timestamp for the current run.
        self._timestamp = datetime.now()

        library = get_one(session, Library, id=self.library_id)
        if not library:
            self.log.error(
                f"Unable to generate report '{self.title}' ({self.key}) for an unknown library (id={self.library_id}): "
                "library not found."
            )
            self.send_error_notification()
            return False
        self._library = library
        try:
            return self._run_report(session=session)
        except Exception as e:
            self.log.exception(
                f"Unable to generate report '{self.title}' ({self.key}) for library '{library.name}': {e}"
            )
            self.send_error_notification()
            return False


class LibraryTitleLevelReport(LibraryCollectionReport):
    KEY = "title-level-report"
    TITLE = "Title-Level Report"
    TABLE_CLASSES = [LibraryAllTitleReportTable]
