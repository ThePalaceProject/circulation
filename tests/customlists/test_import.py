import json
import logging
from pathlib import Path
from typing import List

import pytest

from customlists.customlist_import import CustomListImporter, CustomListImportFailed
from customlists.customlist_report import (
    CustomListProblem,
    CustomListReport,
    CustomListsReport,
)
from tests.core.util.test_mock_web_server import MockAPIServer, MockAPIServerResponse


@pytest.fixture
def mock_web_server():
    """A test fixture that yields a usable mock web server for the lifetime of the test."""
    _server = MockAPIServer("127.0.0.1", 10256)
    _server.start()
    logging.info(f"starting mock web server on {_server.address()}:{_server.port()}")
    yield _server
    logging.info(
        f"shutting down mock web server on {_server.address()}:{_server.port()}"
    )
    _server.stop()


class TestImports:
    @staticmethod
    def _customlists_resource_path(name) -> str:
        """The path to the customlists resource with the given filename."""
        base_path = Path(__file__).parent.parent.parent
        resource_path = base_path / "customlists"
        return str(resource_path / name)

    @staticmethod
    def _test_customlists_resource_path(name) -> str:
        """The path to the customlists test resource with the given filename."""
        base_path = Path(__file__).parent.parent
        resource_path = base_path / "customlists" / "files"
        return str(resource_path / name)

    def test_import_auth_fails(self, mock_web_server: MockAPIServer, tmpdir):
        """If the server returns a ~400 error code, signing in fails."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 401
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        schema_path = TestImports._customlists_resource_path("customlists.schema.json")
        schema_report_path = TestImports._customlists_resource_path(
            "customlists-report.schema.json"
        )
        input_file = TestImports._test_customlists_resource_path(
            "example-customlists.json"
        )
        output_file = tmpdir.join("output.json")
        with pytest.raises(
            CustomListImportFailed, match="Failed to sign in: 401 Unauthorized"
        ):
            CustomListImporter.create(
                [
                    "--server",
                    mock_web_server.url("/"),
                    "--username",
                    "someone@example.com",
                    "--password",
                    "12345678",
                    "--schema-file",
                    str(schema_path),
                    "--schema-report-file",
                    str(schema_report_path),
                    "--file",
                    str(input_file),
                    "--output",
                    str(output_file),
                    "-v",
                ]
            ).execute()

    def test_import_cannot_retrieve_custom_lists(
        self, mock_web_server: MockAPIServer, tmpdir
    ):
        """If the server returns a 404 for the custom lists, fail loudly"""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 404
        mock_web_server.enqueue_response("GET", "/admin/custom_lists", lists_response_0)

        schema_path = TestImports._customlists_resource_path("customlists.schema.json")
        schema_report_path = TestImports._customlists_resource_path(
            "customlists-report.schema.json"
        )
        input_file = TestImports._test_customlists_resource_path(
            "example-customlists.json"
        )
        output_file = tmpdir.join("output.json")
        with pytest.raises(
            CustomListImportFailed,
            match="Failed to retrieve custom lists: 404 Not Found",
        ):
            CustomListImporter.create(
                [
                    "--server",
                    mock_web_server.url("/"),
                    "--username",
                    "someone@example.com",
                    "--password",
                    "12345678",
                    "--schema-file",
                    str(schema_path),
                    "--schema-report-file",
                    str(schema_report_path),
                    "--file",
                    str(input_file),
                    "--output",
                    str(output_file),
                    "-v",
                ]
            ).execute()

    def test_import_cannot_update_custom_list(
        self, mock_web_server: MockAPIServer, tmpdir
    ):
        """If the server returns a 500 for a custom list update, report it"""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 200
        lists_response_0.set_content(
            """
    {
    "custom_lists": []
    }
        """.encode(
                "utf-8"
            )
        )
        mock_web_server.enqueue_response("GET", "/admin/custom_lists", lists_response_0)

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 500
        mock_web_server.enqueue_response(
            "POST", "/HAZELNUT/admin/custom_lists", update_response_0
        )

        schema_path = TestImports._customlists_resource_path("customlists.schema.json")
        schema_report_path = TestImports._customlists_resource_path(
            "customlists-report.schema.json"
        )
        input_file = TestImports._test_customlists_resource_path(
            "example-customlists.json"
        )
        output_file = tmpdir.join("output.json")
        CustomListImporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--schema-file",
                str(schema_path),
                "--schema-report-file",
                str(schema_report_path),
                "--file",
                str(input_file),
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        report_document: CustomListsReport
        with open(schema_report_path, "rb") as schema_file:
            with open(output_file, "rb") as report_file:
                schema = json.load(schema_file)
                document = json.load(report_file)
                report_document = CustomListsReport.parse(
                    schema=schema, document=document
                )

        reports: List[CustomListReport] = list(report_document.reports())
        assert 1 == len(reports)
        report = reports[0]
        problems: List[CustomListProblem] = list(report.problems())
        assert 2 == len(problems)
        assert (
            "Book was excluded from list updates due to a problem on the source CM: Something went wrong on the source CM"
            == problems[0].message()
        )
        assert (
            "Failed to update custom list: 500 Internal Server Error"
            == problems[1].message()
        )

    def test_import_cannot_update_existing_list(
        self, mock_web_server: MockAPIServer, tmpdir
    ):
        """If a list already exists, it isn't updated."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 200
        lists_response_0.set_content(
            """
{
    "custom_lists": [
        {
            "collections": [
                {
                    "id": 864,
                    "name": "B2",
                    "protocol":"OPDS for Distributors"
                }
            ],
            "entry_count":1,
            "id":90,
            "name":"Something Else"
        }
    ]
}
        """.encode(
                "utf-8"
            )
        )
        mock_web_server.enqueue_response("GET", "/admin/custom_lists", lists_response_0)

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 500
        mock_web_server.enqueue_response(
            "POST", "/HAZELNUT/admin/custom_lists", update_response_0
        )

        schema_path = TestImports._customlists_resource_path("customlists.schema.json")
        schema_report_path = TestImports._customlists_resource_path(
            "customlists-report.schema.json"
        )
        input_file = TestImports._test_customlists_resource_path(
            "example-customlists.json"
        )
        output_file = tmpdir.join("output.json")
        CustomListImporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--schema-file",
                str(schema_path),
                "--schema-report-file",
                str(schema_report_path),
                "--file",
                str(input_file),
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        report_document: CustomListsReport
        with open(schema_report_path, "rb") as schema_file:
            with open(output_file, "rb") as report_file:
                schema = json.load(schema_file)
                document = json.load(report_file)
                report_document = CustomListsReport.parse(
                    schema=schema, document=document
                )

        reports: List[CustomListReport] = list(report_document.reports())
        assert 1 == len(reports)
        report = reports[0]
        problems: List[CustomListProblem] = list(report.problems())
        assert 2 == len(problems)
        assert (
            "Book was excluded from list updates due to a problem on the source CM: Something went wrong on the source CM"
            == problems[0].message()
        )
        assert (
            "A list with id 90 and name 'Something Else' already exists and won't be modified"
            == problems[1].message()
        )

    def test_import_dry_run(self, mock_web_server: MockAPIServer, tmpdir):
        """If --dry-run is specified, the lists aren't actually updated."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 200
        lists_response_0.set_content(
            """
    {
    "custom_lists": []
    }
        """.encode(
                "utf-8"
            )
        )
        mock_web_server.enqueue_response("GET", "/admin/custom_lists", lists_response_0)

        schema_path = TestImports._customlists_resource_path("customlists.schema.json")
        schema_report_path = TestImports._customlists_resource_path(
            "customlists-report.schema.json"
        )
        input_file = TestImports._test_customlists_resource_path(
            "example-customlists.json"
        )
        output_file = tmpdir.join("output.json")
        CustomListImporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--schema-file",
                str(schema_path),
                "--schema-report-file",
                str(schema_report_path),
                "--file",
                str(input_file),
                "--output",
                str(output_file),
                "-v",
                "--dry-run",
            ]
        ).execute()

        report_document: CustomListsReport
        with open(schema_report_path, "rb") as schema_file:
            with open(output_file, "rb") as report_file:
                schema = json.load(schema_file)
                document = json.load(report_file)
                report_document = CustomListsReport.parse(
                    schema=schema, document=document
                )

        reports: List[CustomListReport] = list(report_document.reports())
        assert 1 == len(reports)
        report = reports[0]
        problems: List[CustomListProblem] = list(report.problems())
        assert 1 == len(problems)
        assert (
            "Book was excluded from list updates due to a problem on the source CM: Something went wrong on the source CM"
            == problems[0].message()
        )
