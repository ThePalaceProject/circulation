import pytest

from palace.manager.customlists.exporter import (
    CustomListExporter,
    CustomListExportFailed,
    CustomListExports,
)
from tests.fixtures.webserver import MockAPIServer, MockAPIServerResponse
from tests.manager.customlists.conftest import CustomListsFilesFixture


class TestExports:
    def test_export_auth_fails(self, mock_web_server: MockAPIServer, tmpdir):
        """If the server returns a ~400 error code, signing in fails."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 401
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        output_file = tmpdir.join("output.json")
        with pytest.raises(
            CustomListExportFailed, match="Failed to sign in: 401 Unauthorized"
        ):
            CustomListExporter.create(
                [
                    "--server",
                    mock_web_server.url("/"),
                    "--username",
                    "someone@example.com",
                    "--password",
                    "12345678",
                    "--library-name",
                    "ANYTHING",
                    "--output",
                    str(output_file),
                    "-v",
                ]
            ).execute()

    def test_export_empty_lists(self, mock_web_server: MockAPIServer, tmpdir):
        """If the server returns an empty list, the report is empty."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        list_response = MockAPIServerResponse()
        list_response.status_code = 200
        list_response.set_content(b'{"custom_lists":[]}')
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_lists", list_response
        )

        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--library-name",
                "HAZELNUT",
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        exports = CustomListExports.parse_file(file=output_file)
        assert 0 == exports.size()

    def test_export_nonexistent_library(self, mock_web_server: MockAPIServer, tmpdir):
        """If the library is nonexistent, the export fails."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        list_response = MockAPIServerResponse()
        list_response.status_code = 404
        list_response.set_content(
            b'{"type": "http://librarysimplified.org/terms/problem/library-not-found", "title": "Library not found.", "status": 404, "detail": "No library with the requested name on this server."}'
        )
        mock_web_server.enqueue_response(
            "GET", "/NONEXISTENT/admin/custom_lists", list_response
        )

        output_file = tmpdir.join("output.json")

        with pytest.raises(CustomListExportFailed) as e:
            CustomListExporter.create(
                [
                    "--server",
                    mock_web_server.url("/"),
                    "--username",
                    "someone@example.com",
                    "--password",
                    "12345678",
                    "--library-name",
                    "NONEXISTENT",
                    "--output",
                    str(output_file),
                    "-v",
                ]
            ).execute()

        assert e.value.args[0] == "Failed to retrieve custom lists: 404 Not Found"

    def test_export_list_retrieval_fails(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """If fetching the OPDS feed for a list fails, the list is marked as broken."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        lists_response = MockAPIServerResponse()
        lists_response.status_code = 200
        lists_response.content = customlists_files.sample_data(
            "id90-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_lists", lists_response
        )

        list_response = MockAPIServerResponse()
        list_response.status_code = 404
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_list/90", list_response
        )

        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--library-name",
                "HAZELNUT",
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        exports = CustomListExports.parse_file(file=output_file)

        assert 0 == exports.size()
        result_list = list(exports.problematic_lists())[0]
        assert 90 == result_list.id()
        assert "Something Else" == result_list.name()
        assert "Failed to retrieve custom list 90: 404 Not Found" == result_list.error()

    def test_export_simple_collection(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """A simple collection gives the right results."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        lists_response = MockAPIServerResponse()
        lists_response.status_code = 200
        lists_response.content = customlists_files.sample_data(
            "multiple-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_lists", lists_response
        )

        list_response_0 = MockAPIServerResponse()
        list_response_0.status_code = 200
        list_response_0.set_content(customlists_files.sample_data("feed90.xml"))
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_list/90", list_response_0
        )

        list_response_1 = MockAPIServerResponse()
        list_response_1.status_code = 200
        list_response_1.set_content(customlists_files.sample_data("feed91.xml"))
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_list/91", list_response_1
        )

        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--library-name",
                "HAZELNUT",
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        exports = CustomListExports.parse_file(file=output_file)

        assert 2 == exports.size()
        result_list = list(exports.lists())[0]
        assert 1 == result_list.size()
        assert "HAZELNUT" == result_list.library_id()
        assert 90 == result_list.id()
        assert "Something Else" == result_list.name()

        result_list = list(exports.lists())[1]
        assert 1 == result_list.size()
        assert "HAZELNUT" == result_list.library_id()
        assert 91 == result_list.id()
        assert "Other" == result_list.name()

        book = list(result_list.books())[0]
        assert "urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6" == book.id()
        assert "Chameleon" == book.title()
        assert "URI" == book.id_type()

    def test_export_only_one_list(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """A simple collection gives the right results when only asking for one list."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        lists_response = MockAPIServerResponse()
        lists_response.status_code = 200
        lists_response.content = customlists_files.sample_data(
            "multiple-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_lists", lists_response
        )

        list_response_0 = MockAPIServerResponse()
        list_response_0.status_code = 200
        list_response_0.set_content(customlists_files.sample_data("feed90.xml"))
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_list/90", list_response_0
        )

        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--library-name",
                "HAZELNUT",
                "--output",
                str(output_file),
                "-v",
                "--list",
                "Something Else",
            ]
        ).execute()

        exports = CustomListExports.parse_file(file=output_file)

        assert 1 == exports.size()
        result_list = list(exports.lists())[0]
        assert 1 == result_list.size()
        assert "HAZELNUT" == result_list.library_id()
        assert 90 == result_list.id()
        assert "Something Else" == result_list.name()

        book = list(result_list.books())[0]
        assert "urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6" == book.id()
        assert "Chameleon" == book.title()
        assert "URI" == book.id_type()

    def test_export_multiple_alternates(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """Multiple 'alternate' links don't confuse the exporter."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        lists_response = MockAPIServerResponse()
        lists_response.status_code = 200
        lists_response.content = customlists_files.sample_data(
            "multiple-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_lists", lists_response
        )

        list_response_0 = MockAPIServerResponse()
        list_response_0.status_code = 200
        list_response_0.set_content(
            customlists_files.sample_data("feed90multiple_alternates.xml")
        )
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_list/90", list_response_0
        )

        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--library-name",
                "HAZELNUT",
                "--output",
                str(output_file),
                "-v",
                "--list",
                "Something Else",
            ]
        ).execute()

        exports = CustomListExports.parse_file(file=output_file)

        assert 1 == exports.size()
        result_list = list(exports.lists())[0]
        assert 1 == result_list.size()
        assert "HAZELNUT" == result_list.library_id()
        assert 90 == result_list.id()
        assert "Something Else" == result_list.name()

        book = list(result_list.books())[0]
        assert "urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6" == book.id()
        assert "Chameleon" == book.title()
        assert "URI" == book.id_type()

    def test_export_percent_encoded(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """A simple collection with percent-encoded identifiers gives the right results."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        lists_response = MockAPIServerResponse()
        lists_response.status_code = 200
        lists_response.content = customlists_files.sample_data(
            "multiple-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_lists", lists_response
        )

        list_response_0 = MockAPIServerResponse()
        list_response_0.status_code = 200
        list_response_0.set_content(customlists_files.sample_data("feed90.xml"))
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_list/90", list_response_0
        )

        list_response_1 = MockAPIServerResponse()
        list_response_1.status_code = 200
        list_response_1.set_content(customlists_files.sample_data("feed91.xml"))
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_list/91", list_response_1
        )

        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--library-name",
                "HAZELNUT",
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        exports = CustomListExports.parse_file(file=output_file)

        assert 2 == exports.size()
        result_list = list(exports.lists())[0]
        assert 1 == result_list.size()
        assert "HAZELNUT" == result_list.library_id()
        assert 90 == result_list.id()
        assert "Something Else" == result_list.name()

        result_list = list(exports.lists())[1]
        assert 1 == result_list.size()
        assert "HAZELNUT" == result_list.library_id()
        assert 91 == result_list.id()
        assert "Other" == result_list.name()

        book = list(result_list.books())[0]
        assert "urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6" == book.id()
        assert "Chameleon" == book.title()
        assert "URI" == book.id_type()

    def test_export_percent_identifiers(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """A simple collection gives the right results. This example uses a variety of problematic identifiers."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        lists_response = MockAPIServerResponse()
        lists_response.status_code = 200
        lists_response.content = customlists_files.sample_data(
            "problematic-ids-customlists.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_lists", lists_response
        )

        list_response_0 = MockAPIServerResponse()
        list_response_0.status_code = 200
        list_response_0.set_content(customlists_files.sample_data("feed92.xml"))
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_list/92", list_response_0
        )

        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--library-name",
                "HAZELNUT",
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        exports = CustomListExports.parse_file(file=output_file)

        assert 1 == exports.size()
        result_list = list(exports.lists())[0]
        assert 2 == result_list.size()
        assert "HAZELNUT" == result_list.library_id()
        assert 92 == result_list.id()
        assert "Something Else" == result_list.name()

        book = list(result_list.books())[0]
        assert "urn:isbn:9781250776341" == book.id()
        assert "9781250776341" == book.id_value()
        assert "Snapdragon" == book.title()
        assert "ISBN" == book.id_type()

        book = list(result_list.books())[1]
        assert "urn:librarysimplified.org/terms/id/Axis 360 ID/0026082126" == book.id()
        assert "0026082126" == book.id_value()
        assert "This Town Sleeps" == book.title()
        assert "Axis 360 ID" == book.id_type()

    def test_export_other_identifiers(
        self,
        mock_web_server: MockAPIServer,
        customlists_files: CustomListsFilesFixture,
        tmpdir,
    ):
        """A collection with more problematic identifiers."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        lists_response = MockAPIServerResponse()
        lists_response.status_code = 200
        lists_response.content = customlists_files.sample_data(
            "id90-customlists-response.json"
        )
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_lists", lists_response
        )

        list_response_0 = MockAPIServerResponse()
        list_response_0.status_code = 200
        list_response_0.set_content(
            customlists_files.sample_data("feed90-more-identifiers.xml")
        )
        mock_web_server.enqueue_response(
            "GET", "/HAZELNUT/admin/custom_list/90", list_response_0
        )

        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--library-name",
                "HAZELNUT",
                "--output",
                str(output_file),
                "-v",
            ]
        ).execute()

        exports = CustomListExports.parse_file(file=output_file)

        assert 1 == exports.size()
        result_list = list(exports.lists())[0]

        assert 5 == result_list.size()
        assert "HAZELNUT" == result_list.library_id()
        assert 90 == result_list.id()
        assert "Something Else" == result_list.name()

        book = list(result_list.books())[0]
        assert "urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6" == book.id()
        assert "urn:uuid:9c9c1f5c-6742-47d4-b94c-e77f88ca55f6" == book.id_value()
        assert "Chameleon" == book.title()
        assert "URI" == book.id_type()

        book = list(result_list.books())[1]
        assert "urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03" == book.id()
        assert "urn:uuid:b309844e-7d4e-403e-945b-fbc78acd5e03" == book.id_value()
        assert "Chameleon" == book.title()
        assert "URI" == book.id_type()

        book = list(result_list.books())[2]
        assert (
            "urn:librarysimplified.org/terms/id/Overdrive ID/614ed125-d7e5-4cff-b3de-6b6c90ff853c"
            == book.id()
        )
        assert "614ed125-d7e5-4cff-b3de-6b6c90ff853c" == book.id_value()
        assert "Suspect" == book.title()
        assert "Overdrive ID" == book.id_type()

        book = list(result_list.books())[3]
        assert "http://www.feedbooks.com/book/859" == book.id()
        assert "http://www.feedbooks.com/book/859" == book.id_value()
        assert "The Time Traders" == book.title()
        assert "URI" == book.id_type()

        book = list(result_list.books())[4]
        assert "urn:isbn:9780160938818" == book.id()
        assert "9780160938818" == book.id_value()
        assert (
            "Quadrant Conference, August 1943 : papers and minutes of meetings"
            == book.title()
        )
        assert "ISBN" == book.id_type()
