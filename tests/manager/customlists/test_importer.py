import json

import pytest

from palace.manager.customlists.importer import (
    CustomListImporter,
    CustomListImportFailed,
)
from palace.manager.customlists.report import (
    CustomListProblem,
    CustomListReport,
    CustomListsReport,
)
from tests.fixtures.webserver import MockAPIServer, MockAPIServerResponse
from tests.manager.customlists.conftest import CustomListsFilesFixture


class TestImports:
    def test_import_auth_fails(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """If the server returns a ~400 error code, signing in fails."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 401
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        input_file = customlists_files.sample_path_str("example-customlists.json")
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
                    "--file",
                    str(input_file),
                    "--output",
                    str(output_file),
                    "-v",
                ]
            ).execute()

        assert 1 == len(mock_web_server.requests())

    def test_import_library_nonexistent(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
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
        collection_response_0.content = customlists_files.sample_data(
            "b2-collections-response.json"
        )
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

        input_file = customlists_files.sample_path_str("example-customlists.json")
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
                    "--file",
                    str(input_file),
                    "--output",
                    str(output_file),
                    "-v",
                ]
            ).execute()

            assert e.value.args[0] == "Failed to retrieve custom lists: 404 Not Found"

    def test_import_cannot_retrieve_custom_lists(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """If the server returns a 404 for the custom lists, fail loudly"""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = customlists_files.sample_data(
            "empty-collections-response.json"
        )
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

        input_file = customlists_files.sample_path_str("example-customlists.json")
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
                    "--file",
                    str(input_file),
                    "--output",
                    str(output_file),
                    "-v",
                ]
            ).execute()

        assert 7 == len(mock_web_server.requests())

    def test_import_cannot_update_custom_list(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """If the server returns a 500 for a custom list update, report it"""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = customlists_files.sample_data(
            "b2-collections-response.json"
        )
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
        lists_response_0.content = customlists_files.sample_data(
            "empty-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 500
        mock_web_server.enqueue_response(
            "POST", "/WALNUT/admin/custom_lists", update_response_0
        )

        input_file = customlists_files.sample_path_str("example-customlists.json")
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
                "--file",
                str(input_file),
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        report_document: CustomListsReport
        with open(output_file, "rb") as report_file:
            document = json.load(report_file)
            report_document = CustomListsReport.parse(document=document)

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

    def test_import_cannot_update_existing_list(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """If a list already exists, it isn't updated."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = customlists_files.sample_data(
            "b2-collections-response.json"
        )
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
        lists_response_0.content = customlists_files.sample_data(
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

        input_file = customlists_files.sample_path_str("example-customlists.json")
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
                "--file",
                str(input_file),
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        with open(output_file, "rb") as report_file:
            document = json.load(report_file)
            report_document = CustomListsReport.parse(document=document)

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

    def test_import_dry_run(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """If --dry-run is specified, the lists aren't actually updated."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = customlists_files.sample_data(
            "b2-collections-response.json"
        )
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
        lists_response_0.content = customlists_files.sample_data(
            "empty-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        input_file = customlists_files.sample_path_str("example-customlists.json")
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
                "--file",
                str(input_file),
                "--output",
                str(output_file),
                "-v",
                "--dry-run",
            ]
        ).execute()

        report_document: CustomListsReport
        with open(output_file, "rb") as report_file:
            document = json.load(report_file)
            report_document = CustomListsReport.parse(document=document)

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

    def test_import_error_collection_missing(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """If a collection is missing on the target CM, there's an error in the report."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = customlists_files.sample_data(
            "empty-collections-response.json"
        )
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
        lists_response_0.content = customlists_files.sample_data(
            "empty-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        input_file = customlists_files.sample_path_str("example-customlists.json")
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
                "--file",
                str(input_file),
                "--output",
                str(output_file),
                "-v",
                "--dry-run",
            ]
        ).execute()

        report_document: CustomListsReport
        with open(output_file, "rb") as report_file:
            document = json.load(report_file)
            report_document = CustomListsReport.parse(document=document)

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
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
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
        collection_response_0.content = customlists_files.sample_data(
            "b2-collections-response.json"
        )
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
        lists_response_0.content = customlists_files.sample_data(
            "empty-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/WALNUT/admin/custom_lists", update_response_0
        )

        input_file = customlists_files.sample_path_str("example-customlists.json")
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
                "--file",
                str(input_file),
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        report_document: CustomListsReport
        with open(output_file, "rb") as report_file:
            document = json.load(report_file)
            report_document = CustomListsReport.parse(document=document)

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
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """A missing collection results in an update without that collection."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = customlists_files.sample_data(
            "empty-collections-response.json"
        )
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
        lists_response_0.content = customlists_files.sample_data(
            "empty-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/WALNUT/admin/custom_lists", update_response_0
        )

        input_file = customlists_files.sample_path_str("example-customlists.json")
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
                "--file",
                str(input_file),
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        report_document: CustomListsReport
        with open(output_file, "rb") as report_file:
            document = json.load(report_file)
            report_document = CustomListsReport.parse(document=document)

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

    def test_import_updates_with_failed_collection(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
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
        lists_response_0.content = customlists_files.sample_data(
            "empty-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/WALNUT/admin/custom_lists", update_response_0
        )

        input_file = customlists_files.sample_path_str("example-customlists.json")
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
                    "--file",
                    str(input_file),
                    "--output",
                    str(output_file),
                    "-v",
                ]
            ).execute()

    def test_import_bad_book_identifier(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """A book with a mismatched identifier is caught."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        collection_response_0 = MockAPIServerResponse()
        collection_response_0.status_code = 200
        collection_response_0.content = customlists_files.sample_data(
            "empty-collections-response.json"
        )
        mock_web_server.enqueue_response(
            "GET",
            "/admin/collections",
            collection_response_0,
        )

        work_response_0 = MockAPIServerResponse()
        work_response_0.status_code = 200
        work_response_0.content = customlists_files.sample_data(
            "feed90_different_id.xml"
        )
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
        lists_response_0.content = customlists_files.sample_data(
            "empty-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/WALNUT/admin/custom_lists", lists_response_0
        )

        update_response_0 = MockAPIServerResponse()
        update_response_0.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/WALNUT/admin/custom_lists", update_response_0
        )

        input_file = customlists_files.sample_path_str("example-customlists.json")
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
                "--file",
                str(input_file),
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        report_document: CustomListsReport
        with open(output_file, "rb") as report_file:
            document = json.load(report_file)
            report_document = CustomListsReport.parse(document=document)

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
