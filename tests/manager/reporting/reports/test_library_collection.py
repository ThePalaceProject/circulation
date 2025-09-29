import logging
import zipfile
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta
from io import BytesIO
from unittest.mock import MagicMock, PropertyMock, create_autospec, patch

import freezegun
import pytest

from palace.manager.celery.task import Task
from palace.manager.core.exceptions import IntegrationException
from palace.manager.reporting.model import ReportTable, TabularQueryDefinition
from palace.manager.reporting.reports.library_collection import (
    LibraryCollectionReport,
    TableProcessingResult,
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
        KEY = "test-report"
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

        for_text = "some library"
        subject_format = (
            f"Palace report '{report.title}' for {{library_text}} run at {report.timestamp_email_string()} "
            f"(request id: {report.request_id})"
        )

        # Make sure that the template variable survived f-string expansion ...
        assert "{library_text}" in subject_format
        # ... and that our label text doesn't match the real library name.
        assert for_text != report.library.name

        # If `library_text` is provided, it is used as-is in the subject.
        subject = report.email_subject(library_text=for_text)
        assert subject == subject_format.format(library_text=for_text)

        # Otherwise, the name of the report's library is prefixed with the word "library".
        subject = report.email_subject()
        assert subject == subject_format.format(
            library_text=f"library '{report.library.name}'"
        )

    def test_send_success_notification(
        self, report_fixture: LibraryCollectionReportFixture
    ):
        access_url = "test_download_url"
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
            f"You may access the report here -> {access_url} \n\n"
            "This report will be available to download for 30 days."
        )

        report.send_success_notification(access_url=access_url)

        report_fixture.send_email.assert_called_once()
        args, kwargs = report_fixture.send_email.call_args
        assert len(args) == 0
        assert len(kwargs) == 3
        assert kwargs["receivers"] == report.email_address
        assert kwargs["subject"] == expected_subject
        assert kwargs["text"] == expected_text

    @pytest.mark.parametrize(
        "library_name_param, library_set, expected_email_text, expected_subject",
        [
            pytest.param(
                None,
                True,
                "library 'Report Library'",
                "Palace report 'Test Report' for library 'Report Library' run at 2024-01-01 12:00:00 (request id: test_request_id)",
                id="none-subject-default-library",
            ),
            pytest.param(
                "Custom Library Name",
                True,
                "library 'Custom Library Name'",
                "Palace report 'Test Report' for library 'Custom Library Name' run at 2024-01-01 12:00:00 (request id: test_request_id)",
                id="none-subject-custom-library",
            ),
            pytest.param(
                None,
                False,
                "an unknown library",
                "Palace report 'Test Report' for an unknown library run at 2024-01-01 12:00:00 (request id: test_request_id)",
                id="none-subject-no-library",
            ),
        ],
    )
    def test_send_error_notification(
        self,
        report_fixture: LibraryCollectionReportFixture,
        library_name_param,  # The library name provided on the `send_error_notification` call.
        library_set: bool,  # Has the library been set yet in the report?
        expected_email_text: str,  # The library label as we see it in the email.
        expected_subject: str,
    ):
        report = report_fixture.report
        report._timestamp = datetime(2024, 1, 1, 12, 0, 0)

        if library_set:
            report._library = report_fixture.db.library(name="Report Library")

        report.send_error_notification(library_name=library_name_param)

        report_fixture.send_email.assert_called_once()
        args, kwargs = report_fixture.send_email.call_args
        assert len(args) == 0
        assert kwargs["receivers"] == report.email_address
        assert kwargs["subject"] == expected_subject
        assert (
            kwargs["text"]
            == f"There was an error generating the 'Test Report' report for {expected_email_text}. \n\n"
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

    def test_eligible_collections(self, db: DatabaseTransactionFixture):
        library = db.default_library()
        active = db.default_collection()
        inactive = db.default_inactive_collection()

        # The library has two collections, one of which is inactive.
        assert set(library.associated_collections) == {active, inactive}
        assert library.active_collections == [active]

        # Only the active collections are deemed eligible.
        eligible_collections = LibraryCollectionReport.eligible_collections(library)
        assert len(eligible_collections) == 1
        assert eligible_collections == [active]

    def test_process_table(
        self,
        report_fixture: LibraryCollectionReportFixture,
    ):
        table_rows = [("r1c1",), ("r2c1",)]
        expected_csv_content = "col1\r\nr1c1\r\nr2c1\r\n"

        mock_table = MagicMock(spec=ReportTable)
        mock_definition = MagicMock(
            spec=TabularQueryDefinition,
            key="test-table",
            title="Test Table",
            headings=["col1"],
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

        report = report_fixture.report
        report._library = report_fixture.db.default_library()
        report._timestamp = datetime(2024, 1, 1, 12, 0, 0)

        with ExitStack() as stack:
            result = report._process_table(mock_table, stack=stack)

            assert result.key == "test-table"
            assert (
                result.filename == "palace-test-table-default-2024-01-01T12-00-00.csv"
            )
            assert result.row_count == 2
            assert hasattr(result.content_stream, "read")

            content = result.content_stream.read().decode("utf-8")
            assert content == expected_csv_content

    def test_process_table_error(
        self,
        report_fixture: LibraryCollectionReportFixture,
    ):
        mock_table = MagicMock(spec=ReportTable)
        mock_definition = MagicMock(spec=TabularQueryDefinition, key="error_table")
        type(mock_table).definition = PropertyMock(return_value=mock_definition)

        mock_table.side_effect = ValueError("Failed to generate table data")

        report = report_fixture.report
        report._library = report_fixture.db.default_library()
        report._timestamp = datetime(2024, 1, 1, 12, 0, 0)

        with (
            ExitStack() as stack,
            pytest.raises(ValueError, match="Failed to generate table data"),
        ):
            report._process_table(mock_table, stack=stack)

    def test_package_results(
        self,
        report_fixture: LibraryCollectionReportFixture,
    ):
        # We'll assume two tables in our report this time.
        result1_content = b"header1,header2\r\ndata1,data2\r\n"
        result2_content = b"colA,colB\r\nvalA,valB\r\n"

        result1 = TableProcessingResult(
            key="table1",
            title="Table 1",
            filename="table1.csv",
            row_count=1,
            content_stream=BytesIO(result1_content),
        )
        result2 = TableProcessingResult(
            key="table2",
            title="Table 2",
            filename="table2.csv",
            row_count=1,
            content_stream=BytesIO(result2_content),
        )
        results = [result1, result2]

        report = report_fixture.report

        with ExitStack() as stack:
            package = report._package_results(results, stack=stack)

            with zipfile.ZipFile(package, "r") as archive:
                # Both files are included...
                assert "table1.csv" in archive.namelist()
                assert "table2.csv" in archive.namelist()

                # ... and the contents match.
                with archive.open("table1.csv") as f:
                    assert f.read() == result1_content
                with archive.open("table2.csv") as f:
                    assert f.read() == result2_content

    def test_package_results_empty(
        self,
        report_fixture: LibraryCollectionReportFixture,
    ):
        results: list[TableProcessingResult] = []
        report = report_fixture.report

        with ExitStack() as stack:
            package = report._package_results(results, stack=stack)

            with zipfile.ZipFile(package, "r") as archive:
                assert len(archive.namelist()) == 0

    def test_package_results_rollover(
        self,
        report_fixture: LibraryCollectionReportFixture,
    ):
        """Test that SpooledTemporaryFile rollover works correctly with zipfile.

        I added this test to ensure that the pre-Python 3.11, SpooledTemporaryFile
        behaves correctly when it "rolls over" to on-disk storage.
        TODO: This is probably amply tested in Python 3.11+, so we can probably
         remove once we drop support for Python 3.10. Though there's no harm in
         keeping this test around, either.
        """
        content = b"header1,header2\r\ndata1,data2\r\n"

        result = TableProcessingResult(
            key="test-table",
            title="Test Table",
            filename="test-table.csv",
            row_count=1,
            content_stream=BytesIO(content),
        )
        results = [result]

        report = report_fixture.report
        with ExitStack() as stack:
            package = report._package_results(results, stack=stack)

            # Force rollover to disk...
            package.rollover()  # type: ignore[attr-defined]

            # ... and verify that we are allowed to seek.
            package.seek(0)
            with zipfile.ZipFile(package, "r") as archive:
                assert "test-table.csv" in archive.namelist()

                with archive.open("test-table.csv") as f:
                    assert f.read() == content

    def test_store_package(
        self,
        report_fixture: LibraryCollectionReportFixture,
    ):
        package_content = b"some zipped report content"
        package = BytesIO(package_content)

        report = report_fixture.report
        report._library = report_fixture.db.default_library()
        report._timestamp = datetime(2024, 1, 1, 12, 0, 0)

        expected_url = "https://s3.example.com/test-key"
        report_fixture.s3_service.store_stream.return_value = expected_url

        result_url = report._store_package(package)

        assert result_url == expected_url

        report_fixture.s3_service.store_stream.assert_called_once()
        call_args = report_fixture.s3_service.store_stream.call_args
        assert call_args[1]["content_type"] == "application/zip"
        assert "reports/" in call_args[0][0]

    def test_store_package_failure(
        self,
        report_fixture: LibraryCollectionReportFixture,
    ):
        package = BytesIO(b"any old content")

        report = report_fixture.report
        report._library = report_fixture.db.default_library()
        report._timestamp = datetime(2024, 1, 1, 12, 0, 0)

        # Note: `store_stream` returns None on failure.
        report_fixture.s3_service.store_stream.return_value = None

        with pytest.raises(
            IntegrationException,
            match=r"Failed to store report 'test-report' for library 'default' \(default\) to S3.",
        ):
            report._store_package(package)

    def test_get_table_processor(
        self,
        report_fixture: LibraryCollectionReportFixture,
    ):
        from io import StringIO

        mock_table = MagicMock(spec=ReportTable)
        report = report_fixture.report

        test_rows = [["data1", "data2"], ["data3", "data4"]]
        test_headings = ["header1", "header2"]

        # Preset file parameter.
        output1 = StringIO()
        processor_with_preset_file = report.get_table_processor(
            mock_table, file=output1
        )
        processor_with_preset_file(rows=test_rows, headings=test_headings)

        expected_comma = "header1,header2\r\ndata1,data2\r\ndata3,data4\r\n"
        assert output1.getvalue() == expected_comma

        # Both preset file and custom delimiter.
        output2 = StringIO()
        processor_with_both = report.get_table_processor(
            mock_table, file=output2, delimiter="|"
        )
        processor_with_both(rows=test_rows, headings=test_headings)

        expected_pipe = "header1|header2\r\ndata1|data2\r\ndata3|data4\r\n"
        assert output2.getvalue() == expected_pipe

    def test_initialize_tables(
        self,
        report_fixture: LibraryCollectionReportFixture,
        db: DatabaseTransactionFixture,
    ):
        mock_table_class = MagicMock()
        mock_table_instance = MagicMock(spec=ReportTable)
        mock_table_class.return_value = mock_table_instance

        with patch.object(
            report_fixture.MockLibraryCollectionReport,
            "TABLE_CLASSES",
            [mock_table_class],
        ):
            report = report_fixture.report
            report._library = db.default_library()

            tables = report._initialize_tables(db.session)

            assert len(tables) == 1
            assert tables[0] == mock_table_instance

            mock_table_class.assert_called_once_with(
                session=db.session,
                library_id=db.default_library().id,
                collection_ids=None,
            )


class LibraryCollectionReportRunFixture(LibraryCollectionReportFixture):

    class MockLibraryCollectionReport(LibraryCollectionReport):
        KEY = "test-report"
        TITLE = "Test Report"
        # Note: No _run_report override - uses real implementation

    def __init__(self, db: DatabaseTransactionFixture):
        super().__init__(db)

    def create_mock_table(self, *, should_fail: bool = False):
        """Create a mock table class and instance for testing."""
        mock_table_class = MagicMock()
        mock_table_instance = MagicMock(spec=ReportTable)
        mock_definition = MagicMock(
            spec=TabularQueryDefinition,
            key="test-table",
            title="Test Table",
        )
        type(mock_table_instance).definition = PropertyMock(
            return_value=mock_definition
        )

        if should_fail:
            exception = ValueError("Table processing failed")
            mock_table_instance.side_effect = exception
        else:

            def table_call(processor):
                test_rows = [("data1", "data2"), ("data3", "data4")]
                headings = ["col1", "col2"]
                counted_rows, _ = processor(rows=test_rows, headings=headings)
                return counted_rows, None

            mock_table_instance.side_effect = table_call

        mock_table_class.return_value = mock_table_instance
        return mock_table_class

    @contextmanager
    def with_mock_table(self, *, should_fail: bool = False):
        """Context manager that sets up a mock table and patches TABLE_CLASSES."""
        mock_table_class = self.create_mock_table(should_fail=should_fail)

        with patch.object(
            self.MockLibraryCollectionReport,
            "TABLE_CLASSES",
            [mock_table_class],
        ):
            yield mock_table_class


@pytest.fixture
def report_run_fixture(
    db: DatabaseTransactionFixture,
) -> LibraryCollectionReportRunFixture:
    return LibraryCollectionReportRunFixture(db)


class TestLibraryCollectionRun:

    def test_run_full_integration(
        self,
        report_run_fixture: LibraryCollectionReportRunFixture,
    ):
        with report_run_fixture.with_mock_table():
            report = report_run_fixture.report
            expected_url = "https://s3.example.com/reports/test-report.zip"
            report_run_fixture.s3_service.store_stream.return_value = expected_url
            success = report.run(session=report_run_fixture.db.session)

            assert success is True
            assert report.library == report_run_fixture.db.default_library()
            assert report.timestamp is not None

            report_run_fixture.s3_service.store_stream.assert_called_once()
            report_run_fixture.send_email.assert_called_once()

            email_args = report_run_fixture.send_email.call_args[1]
            assert email_args["receivers"] == report.email_address
            assert (
                "Palace report 'Test Report' for library 'default'"
                in email_args["subject"]
            )
            assert "You may access the report here" in email_args["text"]
            assert expected_url in email_args["text"]

    def test_run_table_processing_failure(
        self,
        report_run_fixture: LibraryCollectionReportRunFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(logging.ERROR)

        with report_run_fixture.with_mock_table(should_fail=True):
            report = report_run_fixture.report
            success = report.run(session=report_run_fixture.db.session)

            assert success is False
            report_run_fixture.send_email.assert_called_once()
            email_args = report_run_fixture.send_email.call_args[1]
            assert report.email_address == email_args["receivers"]
            assert (
                "Palace report 'Test Report' for library 'default'"
                in email_args["subject"]
            )
            assert (
                "There was an error generating the 'Test Report' report for library 'default'."
                in email_args["text"]
            )
            assert "Table processing failed" in caplog.text

    def test_run_packaging_failure(
        self,
        report_run_fixture: LibraryCollectionReportRunFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(logging.ERROR)

        with report_run_fixture.with_mock_table():
            report = report_run_fixture.report
            with patch.object(report, "_package_results") as mock_packaging:
                mock_packaging.side_effect = ValueError("Packaging failed")
                success = report.run(session=report_run_fixture.db.session)

            assert success is False
            report_run_fixture.send_email.assert_called_once()
            email_args = report_run_fixture.send_email.call_args[1]
            assert report.email_address == email_args["receivers"]
            assert (
                "Palace report 'Test Report' for library 'default'"
                in email_args["subject"]
            )
            assert (
                "There was an error generating the 'Test Report' report for library 'default'."
                in email_args["text"]
            )
            assert "Failed to package results" in caplog.text

    def test_run_s3_storage_failure(
        self,
        report_run_fixture: LibraryCollectionReportRunFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(logging.ERROR)

        with report_run_fixture.with_mock_table():
            report = report_run_fixture.report
            report_run_fixture.s3_service.store_stream.return_value = None
            success = report.run(session=report_run_fixture.db.session)

            assert success is False
            report_run_fixture.send_email.assert_called_once()
            email_args = report_run_fixture.send_email.call_args[1]
            assert report.email_address == email_args["receivers"]
            assert (
                "Palace report 'Test Report' for library 'default'"
                in email_args["subject"]
            )
            assert (
                "There was an error generating the 'Test Report' report for library 'default'."
                in email_args["text"]
            )
            assert (
                "Failed to store report 'test-report' for library 'default' (default) to S3"
                in caplog.text
            )

    def test_run_library_not_found(
        self,
        report_run_fixture: LibraryCollectionReportRunFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(logging.ERROR)

        report = report_run_fixture.MockLibraryCollectionReport(
            send_email=report_run_fixture.send_email,
            s3_service=report_run_fixture.s3_service,
            request_id="test_request_id",
            library_id=999999,
            email_address="test@example.com",
        )
        success = report.run(session=report_run_fixture.db.session)

        assert success is False
        report_run_fixture.send_email.assert_called_once()
        email_args = report_run_fixture.send_email.call_args[1]
        assert report.email_address == email_args["receivers"]
        assert (
            "Palace report 'Test Report' for an unknown library"
            in email_args["subject"]
        )
        assert (
            "There was an error generating the 'Test Report' report for an unknown library"
            in email_args["text"]
        )
        assert (
            "Unable to generate report 'Test Report' (test-report) for an unknown library (id=999999)"
            in caplog.text
        )
        assert "library not found" in caplog.text

    def test_run_with_collection_ids(
        self,
        report_run_fixture: LibraryCollectionReportRunFixture,
    ):
        collection1 = report_run_fixture.db.collection()
        collection2 = report_run_fixture.db.collection()
        collection_ids = [collection1.id, collection2.id]

        report = report_run_fixture.MockLibraryCollectionReport(
            send_email=report_run_fixture.send_email,
            s3_service=report_run_fixture.s3_service,
            request_id="test_request_id",
            library_id=report_run_fixture.db.default_library().id,
            collection_ids=collection_ids,
            email_address="test@example.com",
        )

        with report_run_fixture.with_mock_table() as mock_table_class:
            report_run_fixture.s3_service.store_stream.return_value = (
                "https://s3.example.com/test.zip"
            )
            success = report.run(session=report_run_fixture.db.session)

            assert success is True
            mock_table_class.assert_called_once_with(
                session=report_run_fixture.db.session,
                library_id=report_run_fixture.db.default_library().id,
                collection_ids=collection_ids,
            )
