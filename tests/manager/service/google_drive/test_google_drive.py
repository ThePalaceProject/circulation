from __future__ import annotations

import json
from io import BytesIO
from typing import TYPE_CHECKING

import pytest
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence

from palace.manager.service.google_drive.google_drive import GoogleDriveService

if TYPE_CHECKING:
    pass


def drive_service(http: HttpMockSequence) -> GoogleDriveService:
    api_client = build(
        "drive", "v3", credentials=None, http=http
    )  #  type: ignore[call-overload]
    return GoogleDriveService(api_client=api_client)


class TestGoogleDriveService:

    def test_create_nested_folders(
        self,
    ):
        root_folder_id = "parent_folder_id"
        parent_id = "parent-id"
        parent_name = "parent-folder"
        child_id = "child-id"
        child_name = "child-folder"
        http_mock_sequence = HttpMockSequence(
            [
                ({"status": "200"}, '{"files": []}'),
                (
                    {"status": "200"},
                    '{"kind": "drive#file", "id": "parent-id", "name": "parent-folder","mimeType": "application/vnd.google-apps.folder"}',
                ),
                ({"status": "200"}, '{"files": []}'),
                (
                    {"status": "200"},
                    '{"kind": "drive#file", "id": "child-id", "name": "child-folder","mimeType": "application/vnd.google-apps.folder"}',
                ),
            ]
        )
        service = drive_service(http=http_mock_sequence)

        # create a parent and child folder
        folders = [parent_name, child_name]

        folder_results = service.create_nested_folders_if_not_exist(
            folders=folders, parent_folder_id=root_folder_id
        )
        assert len(folder_results) == 2

        parent_folder = folder_results[0]
        child_folder = folder_results[1]
        assert parent_id == parent_folder["id"]
        assert parent_name == parent_folder["name"]
        assert child_id == child_folder["id"]
        assert child_name == child_folder["name"]

        request_seq = http_mock_sequence.request_sequence
        assert json.loads(request_seq[1][2]) == {
            "name": parent_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [root_folder_id],
        }
        assert json.loads(request_seq[3][2]) == {
            "name": child_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }

    def test_create_file(self):
        file_name = "file.txt"
        file_id = "file-id"
        http_mock_sequence = HttpMockSequence(
            [
                ({"status": "200"}, '{"files": []}'),
                (
                    {"status": "200"},
                    json.dumps(
                        {
                            "kind": "drive#file",
                            "id": file_id,
                            "name": file_name,
                            "mimeType": "text/plain",
                        }
                    ),
                ),
            ]
        )
        service = drive_service(http=http_mock_sequence)

        mime_type = "text/plain"

        stored_file = service.create_file(
            file_name=file_name,
            stream=BytesIO(b"Hello world"),
            content_type=mime_type,
        )

        assert stored_file
        assert stored_file["id"]
        assert stored_file["name"] == file_name

        request_seq = http_mock_sequence.request_sequence

        body = str(request_seq[1][2])
        assert '{"name": "' + file_name + '", "parents": []}' in body
        assert "Hello world" in body
        assert f"Content-Type: {mime_type}" in body

    def test_create_existing_file_fails(self):
        file_name = "file.txt"
        file_id = "file-id"
        http_mock_sequence = HttpMockSequence(
            [
                (
                    {"status": "200"},
                    json.dumps(
                        {
                            "files": [
                                {
                                    "kind": "drive#file",
                                    "id": file_id,
                                    "name": file_name,
                                    "mimeType": "text/plain",
                                },
                            ]
                        }
                    ),
                ),
            ]
        )
        service = drive_service(http=http_mock_sequence)

        with pytest.raises(FileExistsError):
            service.create_file(
                file_name=file_name,
                stream=BytesIO(b"Hello world"),
                content_type="text/plain",
            )
