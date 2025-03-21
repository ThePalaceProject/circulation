from __future__ import annotations

import json
import os
import random
import string
from io import BytesIO
from typing import Any
from unittest.mock import MagicMock

import pytest

from palace.manager.service.google_drive.google_drive import GoogleDriveService

service_account_info_json = os.getenv(
    "PALACE_TEST_GOOGLE_DRIVE_SERVICE_ACCOUNT_INFO_JSON"
)


class GoogleDriveTestFixture:
    def __init__(self):

        if not service_account_info_json:
            # the only time this condition should be true is when testing locally and the above env variable is unset
            # or the test coverage analysis is being performed.
            self.google_drive_service = MagicMock()
        else:
            service_account_info: dict[str, Any] = json.loads(service_account_info_json)
            self.google_drive_service = GoogleDriveService(service_account_info)


@pytest.fixture
def google_drive_service_fixture() -> GoogleDriveTestFixture:
    fixture = GoogleDriveTestFixture()
    return fixture


def generate_random_strings(length):
    return "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(length)
    )


class TestGoogleDriveService:

    def test_factory(self):
        google_drive_service = GoogleDriveService.factory(
            service_account_info_json=service_account_info_json,
        )
        assert google_drive_service is not None

    def test_create_nested_folders(
        self, google_drive_service_fixture: GoogleDriveTestFixture
    ):
        service = google_drive_service_fixture.google_drive_service
        parent_name = generate_random_strings(10)

        # start by sanity checking the non-existence of the root folder
        folder_info = service.get_file(name=parent_name)
        # assert folder_info is None

        # create a parent and child folder
        folders = [parent_name, "child_dir"]
        folder_results = service.create_nested_folders_if_not_exist(folders=folders)
        assert len(folder_results) == 2

        parent_folder = folder_results[0]
        parent_folder_id = parent_folder["id"]

        # verify that now exist
        assert service.get_file(name=parent_name)
        assert service.get_file(name="child_dir", parent_folder_id=parent_folder_id)

        # delete the directories that were created.
        service.delete_file(file_id=folder_results[1]["id"])
        # confirm that the child folder is gone
        folder_info = service.get_file(
            name="child_dir", parent_folder_id=parent_folder_id
        )
        assert folder_info is None

        service.delete_file(file_id=parent_folder_id)

        # confirm that the parent folder is gone
        folder_info = service.get_file(parent_name)
        assert folder_info is None

    def test_create_file(self, google_drive_service_fixture: GoogleDriveTestFixture):
        service = google_drive_service_fixture.google_drive_service
        file_name = generate_random_strings(10) + ".txt"

        stored_file = service.create_file(
            file_name=file_name,
            stream=BytesIO(b"Hello world"),
            content_type="text/plain",
        )
        assert stored_file
        assert stored_file["id"]
        assert stored_file["name"] == file_name

        retrieved_info = service.get_file(name=file_name)

        assert stored_file["id"] == retrieved_info["id"]
        assert stored_file["name"] == retrieved_info["name"]
        # clean up
        service.delete_file(file_id=retrieved_info["id"])

    def test_create_existing_file_fails(
        self, google_drive_service_fixture: GoogleDriveTestFixture
    ):
        service = google_drive_service_fixture.google_drive_service
        file_name = generate_random_strings(10) + ".txt"

        stored_file = service.create_file(
            file_name=file_name,
            stream=BytesIO(b"Hello world"),
            content_type="text/plain",
        )
        assert stored_file

        with pytest.raises(FileExistsError):
            service.create_file(
                file_name=file_name,
                stream=BytesIO(b"Hello world"),
                content_type="text/plain",
            )
