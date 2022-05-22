import json
import logging
from pathlib import Path

import pytest

from customlists.customlist_export import (
    CustomListExporter,
    CustomListExportFailed,
    CustomListExports,
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


class TestExports:
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
        with open(TestExports._test_customlists_resource_path(name), "rb") as f:
            return f.read()

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
        mock_web_server.enqueue_response("GET", "/admin/custom_lists", list_response)

        schema_path = TestExports._customlists_resource_path("customlists.schema.json")
        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--output",
                str(output_file),
                "--schema-file",
                schema_path,
                "-v",
            ]
        ).execute()

        exports: CustomListExports
        with open(schema_path, "rb") as schema_file:
            schema_text = json.load(schema_file)
            exports = CustomListExports.parse_file(file=output_file, schema=schema_text)

        assert 0 == exports.size()

    def test_export_list_retrieval_fails(self, mock_web_server: MockAPIServer, tmpdir):
        """If fetching the OPDS feed for a list fails, the list is marked as broken."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        lists_response = MockAPIServerResponse()
        lists_response.status_code = 200
        lists_response.content = TestExports._test_customlists_resource_bytes(
            "id90-customlists-response.json"
        )
        mock_web_server.enqueue_response("GET", "/admin/custom_lists", lists_response)

        list_response = MockAPIServerResponse()
        list_response.status_code = 404
        mock_web_server.enqueue_response("GET", "/admin/custom_list/90", list_response)

        schema_path = TestExports._customlists_resource_path("customlists.schema.json")
        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--output",
                str(output_file),
                "--schema-file",
                schema_path,
                "-v",
            ]
        ).execute()

        exports: CustomListExports
        with open(schema_path, "rb") as schema_file:
            schema_text = json.load(schema_file)
            exports = CustomListExports.parse_file(file=output_file, schema=schema_text)

        assert 0 == exports.size()
        result_list = list(exports.problematic_lists())[0]
        assert 90 == result_list.id()
        assert "Something Else" == result_list.name()
        assert "Failed to retrieve custom list 90: 404 Not Found" == result_list.error()

    def test_export_simple_collection(self, mock_web_server: MockAPIServer, tmpdir):
        """A simple collection gives the right results."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        lists_response = MockAPIServerResponse()
        lists_response.status_code = 200
        lists_response.content = TestExports._test_customlists_resource_bytes(
            "multiple-customlists-response.json"
        )
        mock_web_server.enqueue_response("GET", "/admin/custom_lists", lists_response)

        list_response_0 = MockAPIServerResponse()
        list_response_0.status_code = 200
        with open(TestExports._test_customlists_resource_path("feed90.xml")) as file:
            list_response_0.set_content(file.read().encode("utf-8"))
        mock_web_server.enqueue_response(
            "GET", "/admin/custom_list/90", list_response_0
        )

        list_response_1 = MockAPIServerResponse()
        list_response_1.status_code = 200
        with open(TestExports._test_customlists_resource_path("feed91.xml")) as file:
            list_response_1.set_content(file.read().encode("utf-8"))
        mock_web_server.enqueue_response(
            "GET", "/admin/custom_list/91", list_response_1
        )

        schema_path = TestExports._customlists_resource_path("customlists.schema.json")
        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--output",
                str(output_file),
                "--schema-file",
                schema_path,
                "-v",
            ]
        ).execute()

        exports: CustomListExports
        with open(schema_path, "rb") as schema_file:
            schema_text = json.load(schema_file)
            exports = CustomListExports.parse_file(file=output_file, schema=schema_text)

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

    def test_export_only_one_list(self, mock_web_server: MockAPIServer, tmpdir):
        """A simple collection gives the right results when only asking for one list."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        lists_response = MockAPIServerResponse()
        lists_response.status_code = 200
        lists_response.content = TestExports._test_customlists_resource_bytes(
            "multiple-customlists-response.json"
        )
        mock_web_server.enqueue_response("GET", "/admin/custom_lists", lists_response)

        list_response_0 = MockAPIServerResponse()
        list_response_0.status_code = 200
        with open(TestExports._test_customlists_resource_path("feed90.xml")) as file:
            list_response_0.set_content(file.read().encode("utf-8"))
        mock_web_server.enqueue_response(
            "GET", "/admin/custom_list/90", list_response_0
        )

        schema_path = TestExports._customlists_resource_path("customlists.schema.json")
        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--output",
                str(output_file),
                "--schema-file",
                schema_path,
                "-v",
                "--list",
                "Something Else",
            ]
        ).execute()

        exports: CustomListExports
        with open(schema_path, "rb") as schema_file:
            schema_text = json.load(schema_file)
            exports = CustomListExports.parse_file(file=output_file, schema=schema_text)

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

    def test_export_multiple_alternates(self, mock_web_server: MockAPIServer, tmpdir):
        """Multiple 'alternate' links don't confuse the exporter."""
        sign_response = MockAPIServerResponse()
        sign_response.status_code = 200
        mock_web_server.enqueue_response(
            "POST", "/admin/sign_in_with_password", sign_response
        )

        lists_response = MockAPIServerResponse()
        lists_response.status_code = 200
        lists_response.content = TestExports._test_customlists_resource_bytes(
            "multiple-customlists-response.json"
        )
        mock_web_server.enqueue_response("GET", "/admin/custom_lists", lists_response)

        list_response_0 = MockAPIServerResponse()
        list_response_0.status_code = 200
        with open(
            TestExports._test_customlists_resource_path("feed90multiple_alternates.xml")
        ) as file:
            list_response_0.set_content(file.read().encode("utf-8"))
        mock_web_server.enqueue_response(
            "GET", "/admin/custom_list/90", list_response_0
        )

        schema_path = TestExports._customlists_resource_path("customlists.schema.json")
        output_file = tmpdir.join("output.json")
        CustomListExporter.create(
            [
                "--server",
                mock_web_server.url("/"),
                "--username",
                "someone@example.com",
                "--password",
                "12345678",
                "--output",
                str(output_file),
                "--schema-file",
                schema_path,
                "-v",
                "--list",
                "Something Else",
            ]
        ).execute()

        exports: CustomListExports
        with open(schema_path, "rb") as schema_file:
            schema_text = json.load(schema_file)
            exports = CustomListExports.parse_file(file=output_file, schema=schema_text)

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
