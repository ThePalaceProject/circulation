import os
import tempfile
import zipfile
from datetime import datetime, timedelta
from functools import partial
from io import BytesIO, StringIO
from unittest.mock import MagicMock, PropertyMock, create_autospec, patch

import freezegun
import pytest

from palace.manager.celery.task import Task
from palace.manager.reporting.model import ReportTable, TabularQueryDefinition
from palace.manager.reporting.reports.library_collection import (
    LibraryCollectionReport,
)
from palace.manager.reporting.util import RequestIdLoggerAdapter
from palace.manager.service.email.email import SendEmailCallable
from palace.manager.service.storage.s3 import S3Service
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.s3 import S3ServiceIntegrationFixture


class LibraryCollectionReportFixture:

    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.send_email = MagicMock(spec=SendEmailCallable)
        self.s3_service = MagicMock(spec=S3Service)

    class MockLibraryCollectionReport(LibraryCollectionReport):
        KEY = "test_report"
        TITLE = "Test Report"

        def _run_report(self, *args, **kwargs) -> bool:
            return True

    @property
    def report(self) -> LibraryCollectionReport:
        library = self.db.default_library()
        return self.MockLibraryCollectionReport(
            send_email=self.send_email,
            s3_service=self.s3_service,
            request_id="test_request_id",
            library_id=library.id,
            email_address="test@example.com",
        )


@pytest.fixture
def report_fixture(db: DatabaseTransactionFixture) -> LibraryCollectionReportFixture:
    return LibraryCollectionReportFixture(db)


class TestLibraryCollectionReport:

    def test_properties(self, report_fixture: LibraryCollectionReportFixture):
        report = report_fixture.report
        assert report.key is not None
        assert report.key == report.KEY
        assert report.title is not None
        assert report.title == report.TITLE

    def test_table_classes(self, report_fixture: LibraryCollectionReportFixture):
        assert report_fixture.MockLibraryCollectionReport.TABLE_CLASSES == []

        # We should raise an exception if the class var contains an empty list.
        with pytest.raises(ValueError, match="No table classes defined for report .+"):
            _ = report_fixture.report.table_classes

        # We should raise an exception if the class var contains None.
        with (
            patch.object(
                report_fixture.MockLibraryCollectionReport, "TABLE_CLASSES", None
            ),
            pytest.raises(ValueError, match="No table classes defined for report .+"),
        ):
            _ = report_fixture.report.table_classes

        # Otherwise, the property should reflect the class var.
        with patch.object(
            report_fixture.MockLibraryCollectionReport, "TABLE_CLASSES", [ReportTable]
        ):
            report = report_fixture.report
            assert report.table_classes == [ReportTable]

        # Ensure the patch is removed and the original state restored
        assert report_fixture.MockLibraryCollectionReport.TABLE_CLASSES == []

    def test_library_unset_pre_run(
        self, report_fixture: LibraryCollectionReportFixture
    ):
        with pytest.raises(ValueError, match="Library not set for report .+"):
            report = report_fixture.report
            _ = report.library

    def test_library_set_post_run(
        self,
        report_fixture: LibraryCollectionReportFixture,
        db: DatabaseTransactionFixture,
    ):
        report = report_fixture.report
        report.run(session=db.session)
        assert report.library == db.default_library()

    def test_timestamp_unset_pre_run(
        self, report_fixture: LibraryCollectionReportFixture
    ):
        with pytest.raises(ValueError, match="Timestamp not set for report .+"):
            report = report_fixture.report
            _ = report.timestamp

    @freezegun.freeze_time("2024-01-01 12:00:00")
    def test_timestamp_set_post_run(
        self, report_fixture: LibraryCollectionReportFixture
    ):
        report = report_fixture.report
        start_time = datetime.now()
        check_time = start_time + timedelta(seconds=3)

        with freezegun.freeze_time(start_time):
            report.run(session=report_fixture.db.session)

        # We'll check the timestamp a little later, but it should still
        # reflect the original start time.
        with freezegun.freeze_time(check_time):
            assert report.timestamp == start_time

    @pytest.mark.parametrize(
        "timestamp, expected_string",
        [
            (datetime(2024, 1, 1, 12, 0, 0), "2024-01-01T12-00-00"),
            (datetime(2023, 12, 31, 23, 59, 59), "2023-12-31T23-59-59"),
        ],
        ids=["first", "second"],
    )
    def test_timestamp_filename_string(
        self, report_fixture: LibraryCollectionReportFixture, timestamp, expected_string
    ):
        report = report_fixture.report
        report._timestamp = timestamp

        actual_string = report.timestamp_filename_string()

        assert actual_string == expected_string

    @pytest.mark.parametrize(
        "timestamp, expected_string",
        [
            (datetime(2024, 1, 1, 12, 0, 0), "2024-01-01 12:00:00"),
            (datetime(2023, 12, 31, 23, 59, 59), "2023-12-31 23:59:59"),
        ],
        ids=["first", "second"],
    )
    def test_timestamp_email_string(
        self, report_fixture: LibraryCollectionReportFixture, timestamp, expected_string
    ):
        report = report_fixture.report
        report._timestamp = timestamp

        actual_email_string = report.timestamp_email_string()

        assert actual_email_string == expected_string

    def test_report_file_name(self, report_fixture: LibraryCollectionReportFixture):
        report = report_fixture.report
        report._library = report_fixture.db.default_library()
        report._timestamp = datetime(2024, 1, 1, 12, 0, 0)

        file_name = report.get_filename()

        assert file_name == f"palace-{report.key}-default-2024-01-01T12-00-00"

    def test_email_subject(self, report_fixture: LibraryCollectionReportFixture):
        report = report_fixture.report
        report._timestamp = datetime(2024, 1, 1, 12, 0, 0)
        report._library = report_fixture.db.default_library()

        test_library_name = "Test Library"
        subject_format = (
            f"Palace report '{report.title}' for library '{{library_name}}' run at {report.timestamp_email_string()} "
            f"(request id: {report.request_id})"
        )

        assert "{library_name}" in subject_format
        assert test_library_name != report.library.name

        # If a library_name is provided, it is used in the subject.
        subject = report.email_subject(library_name=test_library_name)
        assert subject == subject_format.format(library_name=test_library_name)

        # Otherwise, the name of the report's library is used.
        subject = report.email_subject()
        assert subject == subject_format.format(library_name=report.library.name)

    def test_send_success_notification(
        self, report_fixture: LibraryCollectionReportFixture
    ):
        download_url = "test_download_url"
        timestamp = datetime(2024, 1, 1, 12, 3, 27)

        report = report_fixture.report
        report._library = report_fixture.db.default_library()
        report._timestamp = timestamp

        expected_subject = (
            f"Palace report '{report.title}' "
            f"for library '{report.library.name}' "
            "run at 2024-01-01 12:03:27 "
            f"(request id: {report.request_id})"
        )
        expected_text = (
            f"Download report here -> {download_url} \n\n"
            "This report will be available to download for 30 days."
        )

        report.send_success_notification(download_url=download_url)

        report_fixture.send_email.assert_called_once()
        args, kwargs = report_fixture.send_email.call_args
        assert len(args) == 0
        assert len(kwargs) == 3
        assert kwargs["receivers"] == report.email_address
        assert kwargs["subject"] == expected_subject
        assert kwargs["text"] == expected_text

    def test_send_error_notificationxx(
        self, report_fixture: LibraryCollectionReportFixture
    ):
        subject = "Error notification subject"

        timestamp = datetime(2024, 1, 1, 12, 3, 27)
        report = report_fixture.report
        report._library = report_fixture.db.default_library()
        report._timestamp = timestamp

    @pytest.mark.parametrize(
        "subject, library_name, library_set, expected_library_name, expected_subject",
        [
            pytest.param(
                None,
                None,
                True,
                "Test Library",
                "Palace report 'Test Report' for library 'Test Library' run at 2024-01-01 12:00:00 (request id: test_request_id)",
                id="none-subject-default-library",
            ),
            pytest.param(
                None,
                "Custom Library Name",
                True,
                "Custom Library Name",
                "Palace report 'Test Report' for library 'Custom Library Name' run at 2024-01-01 12:00:00 (request id: test_request_id)",
                id="none-subject-custom-library",
            ),
            pytest.param(
                "Custom Subject",
                None,
                True,
                "Test Library",
                "Custom Subject",
                id="custom-subject-default-library",
            ),
            pytest.param(
                "Custom Subject",
                "Custom Library Name",
                True,
                "Custom Library Name",
                "Custom Subject",
                id="custom-subject-custom-library",
            ),
            pytest.param(
                None,
                None,
                False,
                "an unknown library",
                "Palace report 'Test Report' for library 'an unknown library' run at 2024-01-01 12:00:00 (request id: test_request_id)",
                id="none-subject-no-library",
            ),
            pytest.param(
                "Explicit Subject",
                None,
                False,
                "an unknown library",
                "Explicit Subject",
                id="custom-subject-no-library",
            ),
        ],
    )
    def test_send_error_notification(
        self,
        report_fixture: LibraryCollectionReportFixture,
        subject,
        library_name,
        library_set,
        expected_library_name,
        expected_subject,
    ):
        report = report_fixture.report
        report._timestamp = datetime(2024, 1, 1, 12, 0, 0)

        if library_set:
            report._library = report_fixture.db.library(name=expected_library_name)

        report.send_error_notification(subject=subject, library_name=library_name)

        report_fixture.send_email.assert_called_once()
        args, kwargs = report_fixture.send_email.call_args
        assert len(args) == 0
        assert kwargs["receivers"] == report.email_address
        assert kwargs["subject"] == expected_subject
        assert (
            kwargs["text"]
            == f"There was an error generating the 'Test Report' report for {expected_library_name}. \n\n"
            "If the issue persists, please contact support."
        )

    def test_from_task(
        self,
        celery_fixture: CeleryFixture,
        report_fixture: LibraryCollectionReportFixture,
        s3_service_integration_fixture: S3ServiceIntegrationFixture,
    ):
        storage_mock = MagicMock(spec=S3Service)
        mock_task = create_autospec(Task)
        mock_task.services.email.send_email = MagicMock(spec=SendEmailCallable)
        mock_task.services.storage.public = MagicMock(spec=S3Service)
        mock_task.services.storage.public.return_value = storage_mock

        request_id = "test_request_id"
        library_id = 1
        email_address = "test@example.com"

        report = report_fixture.MockLibraryCollectionReport.from_task(
            mock_task,
            request_id=request_id,
            library_id=library_id,
            email_address=email_address,
        )

        assert isinstance(report, LibraryCollectionReport)
        assert report.send_email == mock_task.services.email.send_email
        assert report.store_s3_stream == storage_mock.store_stream
        assert report.request_id == request_id
        assert report.library_id == library_id
        assert report.email_address == email_address

    def test_log_property(self, report_fixture: LibraryCollectionReportFixture):
        report = report_fixture.report
        with patch.object(report, "logger") as mock_logger:
            log = report.log
            assert isinstance(log, RequestIdLoggerAdapter)
            mock_logger.assert_called_once()

    def test_report_key_property(self, report_fixture: LibraryCollectionReportFixture):
        report = report_fixture.report
        assert report.key == report.KEY

    def test_report_title_property(
        self, report_fixture: LibraryCollectionReportFixture
    ):
        report = report_fixture.report
        assert report.title == report.TITLE

    def test_eligible_integrations(self, db: DatabaseTransactionFixture):
        library = db.default_library()
        active = db.default_collection()
        inactive = db.default_inactive_collection()

        # The library has two collections, one of which is inactive.
        assert set(library.associated_collections) == {active, inactive}
        assert library.active_collections == [active]
        assert active.is_active is True
        assert inactive.is_active is False

        # Only the active collections are deemed eligible.
        eligible_integrations = LibraryCollectionReport.eligible_integrations(library)
        assert len(eligible_integrations) == 1
        assert eligible_integrations == [active.integration_configuration]

    def test_correct_collections_are_included(
        self, report_fixture: LibraryCollectionReportFixture
    ):
        report = report_fixture.report
        library = report_fixture.db.default_library()
        active = report_fixture.db.default_collection()
        inactive = report_fixture.db.default_inactive_collection()

    @pytest.mark.parametrize(
        "file_content_bytes, name, extension, expected_extension, content_type",
        (
            pytest.param(
                b"This is the report content.",
                "test_library/test_report",
                ".zip",
                ".zip",
                "application/zip",
                id="standard_input",
            ),
            pytest.param(
                b"",
                "another/sub/dir/empty-report-content",
                ".zip",
                ".zip",
                "application/zip",
                id="empty_content",
            ),
            pytest.param(
                b"Report content",
                "dotted-report_name.v2.1",
                None,
                "",
                "plain/text",
                id="no_extension",
            ),
            pytest.param(
                b"Report content",
                "dotted-report_name.v2.1",
                "",
                "",
                "plain/text",
                id="empty_extension",
            ),
        ),
    )
    def test_store_to_s3(
        self,
        report_fixture: LibraryCollectionReportFixture,
        file_content_bytes: bytes,
        name: str,
        extension: str | None,
        expected_extension: str,
        content_type,
    ) -> None:
        """Verify that we interact with the S3 service as expected."""
        report = report_fixture.report

        expected_key = f"{S3Service.DOWNLOADS_PREFIX}/reports/{name}-{report.request_id}{expected_extension}"
        expected_url = f"https://s3.example.com/{expected_key}"

        # extension is None means don't pass the argument.
        store_function = (
            partial(report.store_to_s3, extension=extension)
            if extension is not None
            else partial(report.store_to_s3)
        )

        file_stream = BytesIO(file_content_bytes)
        report_fixture.s3_service.store_stream.return_value = expected_url
        result_url = store_function(
            file=file_stream, name=name, content_type=content_type
        )

        assert result_url == expected_url
        report_fixture.s3_service.store_stream.assert_called_once_with(
            expected_key,
            file_stream,
            content_type=content_type,
        )

    def test_store_to_s3_storage_failure(
        self, report_fixture: LibraryCollectionReportFixture
    ) -> None:
        """An exception during S3 storage is propagated."""
        report = report_fixture.report
        s3_store = report_fixture.s3_service.store_stream
        s3_store.side_effect = Exception("S3 Upload Error")

        file_stream = BytesIO(b"Report data")
        name = "failed_lib/failed_report"
        extension = ".zip"
        content_type = "plain/text"
        expected_key = f"{S3Service.DOWNLOADS_PREFIX}/reports/{name}-{report.request_id}{extension}"

        with pytest.raises(Exception, match="S3 Upload Error"):
            report.store_to_s3(
                file=file_stream,
                name=name,
                extension=extension,
                content_type=content_type,
            )

        s3_store.assert_called_once_with(
            expected_key, file_stream, content_type=content_type
        )

    def test_zip_results(
        self,
        report_fixture: LibraryCollectionReportFixture,
    ):
        table_rows = [("r1c1",), ("r2c1",)]
        expected_csv_content = "col1\r\nr1c1\r\nr2c1\r\n"

        mock_table = MagicMock(spec=ReportTable)
        mock_definition = MagicMock(
            spec=TabularQueryDefinition, id="test_table_id", headings=["col1"]
        )
        type(mock_table).definition = PropertyMock(return_value=mock_definition)

        def report_table_call(processor):
            counted_iterator, write_csv_result = processor(
                rows=table_rows, headings=mock_definition.headings
            )
            assert counted_iterator.count == 2
            assert write_csv_result is None
            return counted_iterator, write_csv_result

        mock_table.side_effect = report_table_call

        # Create a temporary that we can use to mock tempfile.NamedTemporaryFile later.
        # We're not deleting it here, so should ensure it's deleted by the end of the test.
        real_temp_file = tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", delete=False, newline="\n"
        )

        # Setup to zip the result file.
        member_name = "report_part_1.csv"
        zip_buffer = BytesIO()

        try:
            # Call zip_results to write to the archive.
            with (
                zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as archive,
                patch("tempfile.NamedTemporaryFile") as mock_temp_file,
            ):
                mock_temp_file.return_value.__enter__.return_value = real_temp_file
                # Now actually call zip_results.
                report_fixture.report.zip_results(
                    archive=archive, member_name=member_name, table=mock_table
                )

                # Verify the temporary file was written with the correct content.
                with open(
                    real_temp_file.name, encoding="utf-8", newline=""
                ) as temp_file:
                    assert temp_file.read() == expected_csv_content
        finally:
            # Clean up the temporary file, now that we're done with it.
            os.remove(real_temp_file.name)

        # Verify the archive contains the member with the correct content.
        with zipfile.ZipFile(zip_buffer, "r") as read_archive:
            with read_archive.open(member_name) as archive_member:
                assert archive_member.read().decode("utf-8") == expected_csv_content

    @patch("tempfile.NamedTemporaryFile")
    def test_zip_results_table_processing_error(
        self,
        mock_named_temp_file: MagicMock,
        report_fixture: LibraryCollectionReportFixture,
    ):
        """An exception during table processing is propagated."""
        mock_temp_file_buffer = StringIO()
        mock_temp_file_buffer.name = "/tmp/fake_temp_file.csv"
        mock_named_temp_file.return_value.__enter__.return_value = mock_temp_file_buffer

        mock_table = MagicMock(spec=ReportTable)
        type(mock_table).definition = PropertyMock(
            return_value=MagicMock(spec=TabularQueryDefinition, id="test_table_id")
        )
        # Make the report table's callable raise an error.
        mock_table.side_effect = ValueError("Failed to generate table data")

        member_name = "report_part_error.csv"
        zip_buffer = BytesIO()

        with (
            zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as archive,
            pytest.raises(ValueError, match="Failed to generate table data"),
        ):
            mock_archive_write = MagicMock()
            archive.write = mock_archive_write
            report_fixture.report.zip_results(
                archive=archive, member_name=member_name, table=mock_table
            )

        # Ensure nothing was written to the temp file buffer.
        assert mock_temp_file_buffer.getvalue() == ""
        mock_archive_write.assert_not_called()
        mock_named_temp_file.assert_called_once()

    @patch("tempfile.NamedTemporaryFile")
    def test_zip_results_archive_write_error(
        self,
        mock_named_temp_file: MagicMock,
        report_fixture: LibraryCollectionReportFixture,
    ):
        """An exception during archive write is propagated."""
        table_rows = [("r1c1",), ("r2c1",)]
        expected_csv_content = "col1\r\nr1c1\r\nr2c1\r\n"

        mock_temp_file_buffer = StringIO()
        mock_temp_file_buffer.name = "/tmp/fake_temp_file.csv"
        mock_named_temp_file.return_value.__enter__.return_value = mock_temp_file_buffer

        mock_table = MagicMock(spec=ReportTable)
        mock_definition = MagicMock(
            spec=TabularQueryDefinition, id="test_table_id", headings=["col1"]
        )
        type(mock_table).definition = PropertyMock(return_value=mock_definition)

        def report_table_call(processor):
            counted_iterator, write_csv_result = processor(
                rows=table_rows, headings=mock_definition.headings
            )
            assert counted_iterator.count == 2
            assert write_csv_result is None
            return counted_iterator, write_csv_result

        mock_table.side_effect = report_table_call

        member_name = "report_part_io_error.csv"

        zip_buffer = BytesIO()
        mock_archive_write = MagicMock(side_effect=IOError("Disk full"))

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as archive:
            archive.write = mock_archive_write
            with pytest.raises(IOError, match="Disk full"):
                report_fixture.report.zip_results(
                    archive=archive, member_name=member_name, table=mock_table
                )

        assert mock_temp_file_buffer.getvalue() == expected_csv_content
        mock_archive_write.assert_called_once_with(
            filename=mock_temp_file_buffer.name, arcname=member_name
        )
        archive.close()

        mock_named_temp_file.assert_called_once()
