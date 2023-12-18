import json
from pathlib import Path

import pytest

from customlists.customlist_import import CustomListImporter, CustomListImportFailed
from customlists.customlist_report import (
    CustomListProblem,
    CustomListReport,
    CustomListsReport,
)
from tests.fixtures.webserver import MockAPIServer, MockAPIServerResponse


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

    @staticmethod
    def _test_customlists_resource_bytes(name) -> bytes:
        with open(TestImports._test_customlists_resource_path(name), "rb") as f:
            return f.read()

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
                    "--library-name",
                    "WALNUT",
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

        assert 1 == len(mock_web_server.requests())

    def test_import_library_nonexistent(self, mock_web_server: MockAPIServer, tmpdir):
        """If the target library does not exist, importing fails."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        sign_response.headers[
            "Set-Cookie"
        ] = "csrf_token=DUZ8inJjpISkyCYjHx7PONZM8354pCu4; HttpOnly; Path=/"
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = self.normalCollections()
        mock_web_server.enqueue_response(
            "GET",
            "/admin/collections",
            collection_response_0,
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        work_response_2 = MockAPIServerResponse()
        work_response_2.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/Overdrive%20ID/614ed125-d7e5-4cff-b3de-6b6c90ff853c",
            work_response_2,
        )

        work_response_3 = MockAPIServerResponse()
        work_response_3.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/http://www.feedbooks.com/book/859",
            work_response_3,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 404
        lists_response_0.content = b"No!"
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/WALNUT/admin/custom_lists", update_response_0
        )

        schema_path = TestImports._customlists_resource_path("customlists.schema.json")
        schema_report_path = TestImports._customlists_resource_path(
            "customlists-report.schema.json"
        )
        input_file = TestImports._test_customlists_resource_path(
            "example-customlists.json"
        )
        output_file = tmpdir.join("output.json")

        with pytest.raises(CustomListImportFailed) as e:
            CustomListImporter.create(
                [
                    "--server",
                    mock_web_server.url("/"),
                    "--username",
                    "someone@example.com",
                    "--password",
                    "12345678",
                    "--library-name",
                    "WALNUT",
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

            assert e.value.args[0] == "Failed to retrieve custom lists: 404 Not Found"

    def test_import_cannot_retrieve_custom_lists(
        self, mock_web_server: MockAPIServer, tmpdir
    ):
        """If the server returns a 404 for the custom lists, fail loudly"""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = self.emptyCollections()
        mock_web_server.enqueue_response(
            "GET",
            "/admin/collections",
            collection_response_0,
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        work_response_2 = MockAPIServerResponse()
        work_response_2.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/Overdrive%20ID/614ed125-d7e5-4cff-b3de-6b6c90ff853c",
            work_response_2,
        )

        work_response_3 = MockAPIServerResponse()
        work_response_3.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/http://www.feedbooks.com/book/859",
            work_response_3,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 404
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
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
                    "--library-name",
                    "WALNUT",
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

        assert 7 == len(mock_web_server.requests())

    def test_import_cannot_update_custom_list(
        self, mock_web_server: MockAPIServer, tmpdir
    ):
        """If the server returns a 500 for a custom list update, report it"""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = self.normalCollections()
        mock_web_server.enqueue_response(
            "GET",
            "/admin/collections",
            collection_response_0,
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        work_response_2 = MockAPIServerResponse()
        work_response_2.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/Overdrive%20ID/614ed125-d7e5-4cff-b3de-6b6c90ff853c",
            work_response_2,
        )

        work_response_3 = MockAPIServerResponse()
        work_response_3.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/http://www.feedbooks.com/book/859",
            work_response_3,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 200
        lists_response_0.content = self.emptyCustomlists()
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 500
        mock_web_server.enqueue_response(
            "POST", "/WALNUT/admin/custom_lists", update_response_0
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
                "--library-name",
                "WALNUT",
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

        reports: list[CustomListReport] = list(report_document.reports())
        assert 1 == len(reports)
        report = reports[0]
        problems: list[CustomListProblem] = list(report.problems())
        assert 2 == len(problems)
        assert (
            "Book 'Bad Book' (urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f7) was excluded from list updates due to a problem on the source CM: Something went wrong on the source CM"
            == problems[0].message()
        )
        assert (
            "Failed to update custom list: 500 Internal Server Error"
            == problems[1].message()
        )

        assert 8 == len(mock_web_server.requests())

    def emptyCustomlists(self):
        return TestImports._test_customlists_resource_bytes(
            "empty-customlists-response.json"
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

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = self.normalCollections()
        mock_web_server.enqueue_response(
            "GET",
            "/admin/collections",
            collection_response_0,
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        work_response_2 = MockAPIServerResponse()
        work_response_2.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/Overdrive%20ID/614ed125-d7e5-4cff-b3de-6b6c90ff853c",
            work_response_2,
        )

        work_response_3 = MockAPIServerResponse()
        work_response_3.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/http://www.feedbooks.com/book/859",
            work_response_3,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 200
        lists_response_0.content = TestImports._test_customlists_resource_bytes(
            "id90-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 500
        mock_web_server.enqueue_response(
            "POST", "/WALNUT/admin/custom_lists", update_response_0
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
                "--library-name",
                "WALNUT",
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

        reports: list[CustomListReport] = list(report_document.reports())
        assert 1 == len(reports)
        report = reports[0]
        problems: list[CustomListProblem] = list(report.problems())
        assert 2 == len(problems)
        assert (
            "Book 'Bad Book' (urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f7) was excluded from list updates due to a problem on the source CM: Something went wrong on the source CM"
            == problems[0].message()
        )
        assert (
            "A list with id 90 and name 'Something Else' already exists and won't be modified"
            == problems[1].message()
        )

        assert 7 == len(mock_web_server.requests())

    def test_import_dry_run(self, mock_web_server: MockAPIServer, tmpdir):
        """If --dry-run is specified, the lists aren't actually updated."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = self.normalCollections()
        mock_web_server.enqueue_response(
            "GET",
            "/admin/collections",
            collection_response_0,
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        work_response_2 = MockAPIServerResponse()
        work_response_2.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/Overdrive%20ID/614ed125-d7e5-4cff-b3de-6b6c90ff853c",
            work_response_2,
        )

        work_response_3 = MockAPIServerResponse()
        work_response_3.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/http://www.feedbooks.com/book/859",
            work_response_3,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 200
        lists_response_0.content = self.emptyCustomlists()
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
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
                "--library-name",
                "WALNUT",
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

        reports: list[CustomListReport] = list(report_document.reports())
        assert 1 == len(reports)
        report = reports[0]
        problems: list[CustomListProblem] = list(report.problems())
        assert 1 == len(problems)
        assert (
            "Book 'Bad Book' (urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f7) was excluded from list updates due to a problem on the source CM: Something went wrong on the source CM"
            == problems[0].message()
        )

        assert 7 == len(mock_web_server.requests())

    def normalCollections(self):
        return TestImports._test_customlists_resource_bytes(
            "b2-collections-response.json"
        )

    def test_import_error_collection_missing(
        self, mock_web_server: MockAPIServer, tmpdir
    ):
        """If a collection is missing on the target CM, there's an error in the report."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = self.emptyCollections()
        mock_web_server.enqueue_response(
            "GET",
            "/admin/collections",
            collection_response_0,
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        work_response_2 = MockAPIServerResponse()
        work_response_2.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/Overdrive%20ID/614ed125-d7e5-4cff-b3de-6b6c90ff853c",
            work_response_2,
        )

        work_response_3 = MockAPIServerResponse()
        work_response_3.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/http://www.feedbooks.com/book/859",
            work_response_3,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 200
        lists_response_0.content = self.emptyCustomlists()
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
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
                "--library-name",
                "WALNUT",
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

        reports: list[CustomListReport] = list(report_document.reports())
        assert 1 == len(reports)
        report = reports[0]
        problems: list[CustomListProblem] = list(report.problems())
        assert 2 == len(problems)
        assert (
            "The collection 'B2' appears to be missing on the importing CM"
            == problems[0].message()
        )
        assert (
            "Book 'Bad Book' (urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f7) was excluded from list updates due to a problem on the source CM: Something went wrong on the source CM"
            == problems[1].message()
        )
        assert 7 == len(mock_web_server.requests())

    def test_import_updates_and_includes_csrf(
        self, mock_web_server: MockAPIServer, tmpdir
    ):
        """Lists are correctly updated and requests include CSRF tokens."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        sign_response.headers[
            "Set-Cookie"
        ] = "csrf_token=DUZ8inJjpISkyCYjHx7PONZM8354pCu4; HttpOnly; Path=/"
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = self.normalCollections()
        mock_web_server.enqueue_response(
            "GET",
            "/admin/collections",
            collection_response_0,
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        work_response_2 = MockAPIServerResponse()
        work_response_2.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/Overdrive%20ID/614ed125-d7e5-4cff-b3de-6b6c90ff853c",
            work_response_2,
        )

        work_response_3 = MockAPIServerResponse()
        work_response_3.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/http://www.feedbooks.com/book/859",
            work_response_3,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 200
        lists_response_0.content = self.emptyCustomlists()
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/WALNUT/admin/custom_lists", update_response_0
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
                "--library-name",
                "WALNUT",
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

        reports: list[CustomListReport] = list(report_document.reports())
        assert 1 == len(reports)
        report = reports[0]
        problems: list[CustomListProblem] = list(report.problems())
        assert 1 == len(problems)
        assert (
            "Book 'Bad Book' (urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f7) was excluded from list updates due to a problem on the source CM: Something went wrong on the source CM"
            == problems[0].message()
        )

        assert 8 == len(mock_web_server.requests())
        req = mock_web_server.requests()[7]
        assert "/WALNUT/admin/custom_lists" == req.path
        assert "POST" == req.method
        assert "DUZ8inJjpISkyCYjHx7PONZM8354pCu4" == req.headers["X-CSRF-Token"]

    def test_import_updates_with_missing_collection(
        self, mock_web_server: MockAPIServer, tmpdir
    ):
        """A missing collection results in an update without that collection."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = self.emptyCollections()
        mock_web_server.enqueue_response(
            "GET",
            "/admin/collections",
            collection_response_0,
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        work_response_2 = MockAPIServerResponse()
        work_response_2.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/Overdrive%20ID/614ed125-d7e5-4cff-b3de-6b6c90ff853c",
            work_response_2,
        )

        work_response_3 = MockAPIServerResponse()
        work_response_3.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/http://www.feedbooks.com/book/859",
            work_response_3,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 200
        lists_response_0.content = self.emptyCustomlists()
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/WALNUT/admin/custom_lists", update_response_0
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
                "--library-name",
                "WALNUT",
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

        reports: list[CustomListReport] = list(report_document.reports())
        assert 1 == len(reports)
        report = reports[0]
        problems: list[CustomListProblem] = list(report.problems())
        assert 2 == len(problems)
        assert (
            "The collection 'B2' appears to be missing on the importing CM"
            == problems[0].message()
        )
        assert (
            "Book 'Bad Book' (urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f7) was excluded from list updates due to a problem on the source CM: Something went wrong on the source CM"
            == problems[1].message()
        )

        assert 8 == len(mock_web_server.requests())
        req = mock_web_server.requests()[7]
        assert "/WALNUT/admin/custom_lists" == req.path

    def emptyCollections(self):
        return TestImports._test_customlists_resource_bytes(
            "empty-collections-response.json"
        )

    def test_import_updates_with_failed_collection(
        self, mock_web_server: MockAPIServer, tmpdir
    ):
        """A failed collection results in an update without that collection."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 500
        mock_web_server.enqueue_response(
            "GET",
            "/admin/collections",
            collection_response_0,
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
        lists_response_0.content = self.emptyCustomlists()
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/WALNUT/admin/custom_lists", update_response_0
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
            CustomListImportFailed,
            match="Unable to retrieve collections: 500 Internal Server Error",
        ):
            CustomListImporter.create(
                [
                    "--server",
                    mock_web_server.url("/"),
                    "--username",
                    "someone@example.com",
                    "--password",
                    "12345678",
                    "--library-name",
                    "WALNUT",
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

    def test_import_bad_book_identifier(self, mock_web_server: MockAPIServer, tmpdir):
        """A book with a mismatched identifier is caught."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = self.emptyCollections()
        mock_web_server.enqueue_response(
            "GET",
            "/admin/collections",
            collection_response_0,
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        with open(
            TestImports._test_customlists_resource_path("feed90_different_id.xml"), "rb"
        ) as f:
            work_response_0.content = f.read()
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6",
            work_response_0,
        )

        work_response_1 = MockAPIServerResponse()
        work_response_1.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03",
            work_response_1,
        )

        work_response_2 = MockAPIServerResponse()
        work_response_2.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/Overdrive%20ID/614ed125-d7e5-4cff-b3de-6b6c90ff853c",
            work_response_2,
        )

        work_response_3 = MockAPIServerResponse()
        work_response_3.status_code = 200
        mock_web_server.enqueue_response(
            "GET",
            "/WALNUT/admin/works/URI/http://www.feedbooks.com/book/859",
            work_response_3,
        )

        lists_response_0 = MockAPIServerResponse()
        lists_response_0.status_code = 200
        lists_response_0.content = self.emptyCustomlists()
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/WALNUT/admin/custom_lists", update_response_0
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
                "--library-name",
                "WALNUT",
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

        reports: list[CustomListReport] = list(report_document.reports())
        assert 1 == len(reports)
        report = reports[0]
        problems: list[CustomListProblem] = list(report.problems())
        assert 3 == len(problems)
        assert (
            "The collection 'B2' appears to be missing on the importing CM"
            == problems[0].message()
        )
        assert (
            "Book is mismatched on the importing CM. Expected title is 'Chameleon', received title is 'Chameleon'. Expected ID is 'urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6', received ID is 'urn:uuid:eff86500-009d-4e64-b675-0c0b1b6f243d'."
            == problems[1].message()
        )
        assert (
            "Book 'Bad Book' (urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f7) was excluded from list updates due to a problem on the source CM: Something went wrong on the source CM"
            == problems[2].message()
        )

        assert 8 == len(mock_web_server.requests())
        req = mock_web_server.requests()[7]
        assert "/WALNUT/admin/custom_lists" == req.path
